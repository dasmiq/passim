import argparse
import heapq as hq
import json, os, sys
from collections import deque
from intervaltree import Interval, IntervalTree
from math import ceil, log, inf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (col, desc, explode, size, udf, struct, length,
                                   collect_list, collect_set, sort_array, when,
                                   expr, map_from_entries, flatten, xxhash64, lit,
                                   array, arrays_zip)
import pyspark.sql.functions as f
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import RFormula, SQLTransformer
from graphframes import GraphFrame

from dataclasses import dataclass

def getPostings(text, n, floating_ngrams):
    tf = dict()
    posts = list()
    start = deque([], maxlen=n)
    chars = deque([], maxlen=n)
    for i, c in enumerate(text):
        if c.isalnum():
            prev = start[0] if len(start) == n else -2
            start.append(i)
            chars.append(c.lower())
            if len(chars) == n and ( floating_ngrams or (start[0] - prev) > 1 ):
                key = ''.join(chars)
                val = tf.get(key, 0) + 1
                tf[key] = val
                if val == 1:
                    posts.append((key, start[0]))
    return [(key, i) for key, i in posts if tf[key] == 1]

def mergePosts(posts):
    matches = dict()
    for plist in posts:
        for p in plist:
            key = (p.post2, p.df)
            val = matches.get(key, list())
            val.extend(p.alg)
            matches[key] = val
    res = [(k[0], k[1], v) for k, v in matches.items()]
    res.sort()
    return res

def vitSrc(pos, prior, n, max_gap, min_align):
    @dataclass(frozen=True)
    class BP:
        lab: int
        pos2: int
        pos: int

    @dataclass(frozen=True)
    class Score:
        pos2: int
        pos: int
        score: float

    lZ = log(sum(prior.values()))
    lprior = {k:(log(v) - lZ) for k, v in prior.items()}

    ## b70 docs: 21918104; 237921873725 characters
    N = 100 # 21918104
    pcopy = 0.8
    pcont = 0.998
    V = 256
    lpcopy = log(pcopy)
    lpmiss = log(1 - pcopy) - log(2 * V)
    lpstop = log((1 + pcopy) / 2)
    lpcont = log(pcont)
    lpswitch = log(1 - pcont)

    ## Spans can only begin or end at matches
    ## Should probably just use start points in dynamic program
    bp = dict()
    prev = {}
    bgscore = [Score(0, 0, 0.0)]
    last = 0
    for p in pos:
        npos = p.post2
        stride = npos - last
        i = len(bgscore) - 1
        while i >= 0 and bgscore[i].pos2 > npos:
            i -= 1
        bglast = bgscore[i]
        bg = bglast.score + (p.post2 - bglast.pos2) * (lpcont + -log(V))
        # prev[0] = Score(npos + n, 0, bg + log(p.df) - log(N))
        prev[0] = Score(npos + n, 0, bg + n * (lpcont + -log(V)))
        bp[BP(0, npos + n, 0)] = BP(0, bglast.pos2, 0)
        # print(prev[0], file=sys.stderr)
        for m in p.alg:
            same = prev.get(m.uid, Score(0, 0, -inf))
            gap = max(0, m.post - same.pos - n)
            gap2 = max(0, p.post2 - same.pos2 - n)
            # Show we treat gaps on the source side the same? What about retrogrades?
            overlap = min(gap, gap2)
            minsub = ceil(overlap / n)
            minerr = minsub + gap2 - overlap
            # minerr = minsub + abs(gap2 - gap)
            cont = same.score + gap2 * lpcont + minerr * lpmiss + (overlap - minsub) * lpcopy \
                + min(n, npos - same.pos2) * (lpcont + lpcopy)
            switch = bg + lpswitch + lprior.get(m.uid, log(0.001)) + n * (lpcont + lpcopy)
            if (m.post < same.pos or gap2 > max_gap or gap > max_gap
                or (switch > cont and stride > n)):
                score = switch
                bp[BP(m.uid, npos, m.post)] = BP(0, bglast.pos2, 0)
            else:
                score = cont
                bp[BP(m.uid, npos, m.post)] = BP(m.uid, same.pos2, same.pos)
            prev[m.uid] = Score(npos, m.post, score)
            stop = score + lpstop + lpswitch
            if stop > prev[0].score:
                prev[0] = Score(npos + n, 0, stop)
                # print("# Stop %d @ %d: " % (m.uid, npos) + str(prev), file=sys.stderr)
                bp[BP(0, npos + n, 0)] = BP(m.uid, npos, m.post)
        last = npos
        bgscore.append(prev[0])
        # print("%d: " % npos, prev, file=sys.stderr)
        # print(bp, file=sys.stderr)

    matches = list()
    if len(bp) > 0:
        cur = BP(0, last + n, 0)
        while cur.pos2 > 0:
            matches.append(cur)
            cur = bp[cur]
    matches.reverse()

    # return [(m.lab, m.pos2, m.pos2 + n, m.pos, m.pos + n) for m in matches]

    ## Merge matches into spans
    spans = list()
    i = 0
    while i < len(matches):
        if matches[i].lab != 0:
            start = matches[i]
            m = list()
            j = i
            while j < len(matches):
                if matches[j].lab == 0:
                    if False and ((j+1) < len(matches) and matches[j+1].lab == matches[i].lab and
                        ## Allow singleton retrograde alignments
                        (matches[j+1].pos >= matches[i].pos
                         and (matches[j+1].pos2 - matches[j].pos2) < max_gap)):
                        j += 1
                    else:
                        if (matches[j-1].pos2 - matches[i].pos2 + n) >= min_align:
                            spans.append((matches[i].lab, matches[i].pos2, matches[j-1].pos2 + n,
                                          matches[i].pos, matches[j-1].pos + n, m))
                        break
                m.append((matches[j].pos2, matches[j].pos))
                j += 1
            i = j
        i += 1

    return spans

def srcStats(src, docs):
    N = len(src)
    counts = dict()
    for d in docs:
        counts[d] = 0
    for s in src:
        counts[s.uid] = counts.get(s.uid, 0) + 1
    return [(uid, [(1, count), (0, N - count)]) for uid, count in counts.items()]

def spanEdge(src, max_gap):
    res = list()
    for i, s in enumerate(src):
        lend = src[i-1].end2 if i > 0 else 0
        left2 = max(lend, s.begin2 - max_gap)
        rend = src[i+1].begin2 if (i + 1) < len(src) else s.end2 + max_gap
        right2 = min(rend, s.end2 + max_gap)
        left = max(0, s.begin - (s.begin2 - left2 + 20))
        right = s.end + 20 + (right2 - s.end2)
        res.append((s.uid, left2, s.begin2, s.end2, right2, left, s.begin, s.end, right, s.anchors))
    return res

def anchorAlign(config, s1, s2, side):
    if side == 'left':
        s1 = s1[::-1]
        s2 = s2[::-1]

    V = 256
    logV = log(V)
    pcopy = 0.8
    lpcopy = log(pcopy)
    lpedit = log(1 - pcopy) - log(2 * V)
    lpstop = log((1 + pcopy) / 2)
    pfinal = 0.01
    lpfinal = log(pfinal)
    lppad = log(1 - pfinal) - log(V)

    #print(len(s1), s1)
    #print(len(s2), s2)
    #return "done!"

    chart = {}
    pq = [(0, (0, 0, 0, 0))]
    while len(pq) > 0:
        #print(pq)
        top = hq.heappop(pq)
        #print(top)
        (score, item) = top
        (s, t, e1, e2) = item
        if s == len(s1) and t == len(s2):
            break
        ## Finish
        cand = [(score - (lpstop + 2 * lpfinal + lppad * (len(s2) - t)),
                  (len(s1), len(s2), s, t))]
        if s < len(s1):         # delete
            cand.append((score - lpedit, (s + 1, t, e1, e2)))
            if t < len(s2):
                if s1[s].lower() == s2[t].lower() or (s1[s].isspace() and s2[t].isspace()): #copy
                    cand.append((score - lpcopy, (s + 1, t + 1, e1, e2)))
                elif s1[s] != '\n' and s2[t] != '\n':
                    cand.append((score - lpedit, (s + 1, t + 1, e1, e2)))
        if t < len(s2):         # insert
            cand.append((score - lpedit, (s, t + 1, e1, e1))) # e1 typo?
        for c in cand:
            (score, item) = c
            if item[2] == 0 and abs(item[0] - item[1]) > config.max_offset:
                continue
            if chart.get(item, inf) > score:
                chart[item] = score
                hq.heappush(pq, c)
                
    (score, (s, t, e1, e2)) = top
    # It might be worth deleting small numbers of characters trailing (leading) a newline.
    if side == 'left':
        return (s1[e1-1::-1], s2[e2-1::-1])
    else:
        return (s1[0:e1], s2[0:e2])

def levAlign(config, s1, s2):
    V = 256
    logV = log(V)
    pcopy = 0.8
    lpcopy = log(pcopy)
    lpedit = log(1 - pcopy) - log(2 * V)

    s1 = s1.replace('-', '\u2010')
    s2 = s2.replace('-', '\u2010')
    if len(s1) == len(s2) and s1 == s2: # There's probably a length break-even point here.
        return (s1, s2)

    chart = {}
    bp = {}
    pq = [(0, (0, 0))]
    while len(pq) > 0:
        #print(pq)
        top = hq.heappop(pq)
        #print(top)
        (score, item) = top
        (e1, e2) = item
        if e1 == len(s1) and e2 == len(s2):
            break
        cand = list()
        if e1 < len(s1):         # delete
            cand.append((score - lpedit, (e1 + 1, e2)))
            if e2 < len(s2):
                if s1[e1].lower() == s2[e2].lower() or (s1[e1].isspace() and s2[e2].isspace()): #copy
                    cand.append((score - lpcopy, (e1 + 1, e2 + 1)))
                elif s1[e1] != '\n' and s2[e2] != '\n':
                    cand.append((score - lpedit, (e1 + 1, e2 + 1)))
        if e2 < len(s2):         # insert
            cand.append((score - lpedit, (e1, e2 + 1)))
        for c in cand:
            (score, item) = c
            if abs(item[0] - item[1]) > config.max_offset:
                continue
            if chart.get(item, inf) > score:
                chart[item] = score
                bp[item] = (e1, e2)
                hq.heappush(pq, c)
                
    (score, (e1, e2)) = top
    if e1 == len(s1) and e2 == len(s2):
        alg1 = list()
        alg2 = list()
        while e1 > 0 or e2 > 0:
            (p1, p2) = bp[(e1, e2)]
            if p1 < e1:
                alg1 += s1[p1]
                if p2 < e2:
                    alg2 += s2[p2]
                else:
                    alg2 += '-'
            else:
                alg1 += '-'
                alg2 += s2[p2]
            e1 = p1
            e2 = p2
        return (''.join(reversed(alg1)), ''.join(reversed(alg2)))
    else:                       # alignment failed
        return (s1 + '-' * len(s2), '-' * len(s1) + s2)

def chunkAlign(config, begin, begin2, text, text2, anchors):
    alg1 = ''
    alg2 = ''
    b1 = 0
    b2 = 0
    for a in anchors:
        e1 = a.pos - begin
        e2 = a.pos2 - begin2
        (s1, s2) = levAlign(config, text[b1:e1], text2[b2:e2])
        alg1 += s1
        alg2 += s2
        b1 = e1
        b2 = e2
    (s1, s2) = levAlign(config, text[b1:len(text)], text2[b2:len(text2)])
    alg1 += s1
    alg2 += s2
    return (alg1, alg2)

def countMatches(s1, s2):
    return len(list(filter(lambda c: c[0] == c[1], zip(s1, s2))))

def targetLines(begin, begin2, alg):
    lines = list()
    start = 0
    b1 = begin
    b2 = begin2
    end = 0
    while end < len(alg.s2):
        if alg.s2[end] == '\n':
            s1 = alg.s1[start:(end+1)]
            s2 = alg.s2[start:(end+1)]
            lines.append((b1, b2, s1, s2, countMatches(s1, s2)))
            b1 += len(s1) - s1.count('-')
            b2 += len(s2) - s2.count('-')
            start = end + 1
        end += 1
    if start < len(alg.s2):
        s1 = alg.s1[start:len(alg.s1)]
        s2 = alg.s2[start:len(alg.s2)]
        lines.append((b1, b2, s1, s2, countMatches(s1, s2)))
    return lines

def loverlap(s1, s2):
    return max(0, min(s1.end, s2.end) - max(s1.begin, s2.begin))

def linkSpans(s1, s2):
    srcOverlap = 0.5
    dstOverlap = 0.9
    sib = IntervalTree([Interval(s[0], s[1]) for s in s1])
    src = IntervalTree([Interval(s[0], s[1]) for s in s2]) if s2 != None else IntervalTree()
    res = list()
    for s in s1:
        cur = Interval(s[0], s[1])
        sources = [i for i in src[cur.begin:cur.end]
                   if loverlap(cur, i) / cur.length() >= srcOverlap]
        # NB: There shouldn't be much overlap in sources, except due to edge alignments;
        # nevertheless, pick the largest overlap.
        sources.sort(key=lambda i: loverlap(cur, i) / cur.length(), reverse=True)
        if sources:
            res.append((cur.begin, cur.end, sources[0].begin, sources[0].end))
        else:
            sibs = [i for i in sib[cur.begin:cur.end] if (i != cur and i.length() <= cur.length())]
            sibs.sort(key=lambda i: loverlap(cur, i) / cur.length(), reverse=True)
            if (sibs and
                (loverlap(cur, sibs[0]) / cur.length()) >= dstOverlap):
                res.append((cur.begin, cur.end, sibs[0].begin, sibs[0].end))
    return res

def mergeSpans(spans, uid):
    spans.sort()
    res = list()
    curBegin = -1
    curEnd = -1
    src = list()
    boiler = False
    for s in spans:
        if s.begin < curEnd and s.end >= curBegin:
            curBegin = min(curBegin, s.begin)
            curEnd = max(curEnd, s.end)
        else:
            if curEnd > 0:
                res.append((curBegin, curEnd, boiler, src))
            curBegin = s.begin
            curEnd = s.end
            src.clear()
            boiler = False
        if s.src != None and s.src.uid != uid:
            src.append(s.src)
            boiler |= s.boiler
    if curEnd > 0:
        res.append((curBegin, curEnd, boiler, src))
    return res

def clusterExtents(config, extents):
    s1 = extents.groupBy('uid').agg(sort_array(collect_set(struct('begin', 'end'))).alias('s1'))
    s2 = extents.groupBy('uid2'
               ).agg(sort_array(collect_set(struct('begin2', 'end2'))).alias('s2'))

    link_spans = udf(lambda s1, s2: linkSpans(s1, s2),
                     'array<struct<begin: int, end: int, begin2: int, end2: int>>')

    within = s1.join(s2, col('uid') == col('uid2'), 'leftouter'
              ).select('uid', explode(link_spans('s1', 's2')).alias('link')
              ).selectExpr('struct(uid, link.begin2 as begin, link.end2 as end) as src',
                           'struct(uid, link.begin, link.end) as dst',
                           'false as boiler'
              ).distinct()

    boiler = 'gid = gid2' if 'gid' in extents.columns else 'false'

    between = extents.selectExpr('struct(uid, begin, end) as src',
                                 'struct(uid2 as uid, begin2 as begin, end2 as end) as dst',
                                 f'{boiler} as boiler')

    vert = extents.selectExpr('struct(uid2 as uid, begin2 as begin, end2 as end) as id',
                 ).union(extents.selectExpr('struct(uid, begin, end) as id')
                 ).distinct()

    edges = between.union(within)

    g = GraphFrame(vert, edges)
    g.cache()

    cc = g.connectedComponents().withColumnRenamed('component', 'cluster')

    merge_spans = udf(lambda spans, uid: mergeSpans(spans, uid),
                      'array<struct<begin: int, end: int, boiler: boolean, src: array<struct<uid: bigint, begin: int, end: int>>>>')
    
    cspans = cc.join(edges, col('id') == col('dst'), 'leftouter'
              ).select(col('id.*'), 'cluster', 'src', 'boiler'
              ).groupBy('cluster', 'uid'
              ).agg(collect_set(struct('begin', 'end', 'boiler', 'src')).alias('spans')
              ).withColumn('spans', explode(merge_spans('spans', 'uid'))
              ).select('cluster', 'uid', col('spans.*'))

    sizes = cspans.groupBy('cluster'
                 ).agg(f.countDistinct('uid').alias('size'),
                       (f.sum(col('boiler').cast('int'))
                        / (f.count('boiler') - 1.0)).alias('pboiler'))
    # Subtract 1 from denominator to account for root

    return cspans.join(sizes, 'cluster')
    
def clusterJoin(config, clusters, corpus):
    out = clusters.join(corpus, 'uid'
                 ).withColumn(config.text,
                              col(config.text).substr(col('begin'), col('end') - col('begin')))

    pageCol = 'pages'
    if pageCol in out.columns:
        pageFields = ', '.join([f'p.{f} as {f}'
                                for f in out.selectExpr(f'inline({pageCol})').columns
                                if f != 'regions'])
        out = out.withColumn(pageCol, expr(f'''
filter(transform({pageCol}, p -> struct({pageFields}, filter(p.regions, r -> r.start < end AND (r.start + r.length) > begin) as regions)), p -> size(p.regions) > 0)''')
                ).withColumn(pageCol, expr(f'''
transform({pageCol},
          p -> struct({pageFields},
                      array(aggregate(p.regions,
                                      struct(p.regions[0].start as start,
                                             p.regions[0].length as length,
                                             struct(p.regions[0].coords.x as x,
                                                    p.regions[0].coords.y as y,
                                                    p.regions[0].coords.w as w,
                                                    p.regions[0].coords.h as h) as coords),
                                      (acc, r) -> struct(least(acc.start, r.start) as start,
                                                         greatest(acc.start + acc.length, r.start + r.length) - least(acc.start, r.start) as length,
                                                         struct(least(acc.coords.x, r.coords.x) as x,
                                                                least(acc.coords.y, r.coords.y) as y,
                                                                greatest(acc.coords.x + acc.coords.w, r.coords.x + r.coords.w) - least(acc.coords.x, r.coords.x) as w,
                                                                greatest(acc.coords.y + acc.coords.h, r.coords.y + r.coords.h) - least(acc.coords.y, r.coords.y) as h) as coords))) as regions))
'''))

    if config.output_format != 'parquet':
        out = out.sort(desc('size'), 'cluster', *[expr(f) for f in config.fields],
                       col(config.id), 'begin')

    return out

def main(args):
    parser = argparse.ArgumentParser(description='Passim Alignment',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--id', type=str, default='id',
                        help='Field for unique document IDs')
    parser.add_argument('-t', '--text', type=str, default='text',
                        help='Field for document text')
    parser.add_argument('-l', '--minDF', type=int, default=2,
                        help='Lower limit on document frequency', metavar='N')
    parser.add_argument('-u', '--maxDF', type=int, default=100,
                        help='Upper limit on document frequency', metavar='N')
    parser.add_argument('-m', '--min-match', type=int, metavar='N', default=5,
                        help='Minimum number of n-gram matches between documents')
    parser.add_argument('-n', '--n', type=int, default=20,
                        help='n-gram order', metavar='N')
    parser.add_argument('--floating-ngrams', action='store_true',
                        help='Allow n-grams to float from word boundaries')
    parser.add_argument('-g', '--gap', type=int, default=600,
                        help='Minimum size of gap that separates passages', metavar='N')
    parser.add_argument('--max-offset', type=int, default=10,
                        help='Maximum offset in global alignment', metavar='N')
    parser.add_argument('-a', '--min-align', type=int, default=50,
                         help='Minimum length of alignment', metavar='N')
    parser.add_argument('--fields', type=str, nargs='+', default=[],
                        help='List of fileds to index')
    parser.add_argument('-f', '--filterpairs', type=str, default='uid < uid2',
                        help='SQL constraint on posting pairs')
    parser.add_argument('--all-pairs', action='store_true',
                        help='Compute alignments for all pairs.')
    parser.add_argument('--linewise', action='store_true',
                        help='Output linewise alignments')
    parser.add_argument('--link-model', type=str, default=None,
                        help='Link model in R format')
    parser.add_argument('--link-features', type=str, default=None,
                        help='Link model features as SQL SELECT')
    parser.add_argument('--input-format', type=str, default='json',
                        help='Input format')
    parser.add_argument('--output-format', type=str, default='json',
                        help='Output format')
    parser.add_argument('inputPath', metavar='<path>', help='input data')
    parser.add_argument('outputPath', metavar='<path>', help='output')
    config = parser.parse_args(args)

    print(config)

    spark = SparkSession.builder.appName('Passim Alignment').getOrCreate()
    spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    spark.conf.set('spark.sql.legacy.parquet.datetimeRebaseModeInWrite', 'CORRECTED')

    dfpostFname = os.path.join(config.outputPath, 'dfpost.parquet')
    pairsFname = os.path.join(config.outputPath, 'pairs.parquet')
    srcFname = os.path.join(config.outputPath, 'src.parquet')
    featFname = os.path.join(config.outputPath, 'feat.parquet')
    extentsFname = os.path.join(config.outputPath, 'extents.parquet')
    psgFname = os.path.join(config.outputPath, 'psg.parquet')
    clustersFname = os.path.join(config.outputPath, 'clusters.parquet')
    outFname = os.path.join(config.outputPath, 'out.' + config.output_format)

    corpus = spark.read.option('mergeSchema',
                               'true').format(config.input_format).load(config.inputPath
                               ).na.drop(subset=[config.id, config.text]
                               ).withColumn('uid', xxhash64(config.id))

    termCorpus = corpus.selectExpr('uid', config.text, *config.fields)
    f1 = [f for f in termCorpus.columns if f != config.text]
    f2 = [f + '2' for f in f1]

    spark.conf.set('spark.sql.shuffle.partitions', corpus.rdd.getNumPartitions() * 3)

    get_postings = udf(lambda text: getPostings(text, config.n, config.floating_ngrams),
                       'array<struct<feat: string, post: int>>')
    
    posts = termCorpus.select(*f1, explode(get_postings(config.text)).alias('post')
                     ).select(*f1, col('post.*')
                     ).withColumn('feat', xxhash64('feat'))

    df = posts.groupBy('feat').count().select('feat', col('count').cast('int').alias('df')
             ).filter( (col('df') >= config.minDF) & (col('df') <= config.maxDF) )

    posts.join(df, 'feat').write.mode('ignore').save(dfpostFname)
    
    dfpost = spark.read.load(dfpostFname)

    merge_posts = udf(lambda posts: mergePosts(posts),
                      'array<struct<post2: int, df: int, alg: array<struct<uid: bigint, post:int >>>>')

    apos = dfpost.join(dfpost.toDF(*[f + ('2' if f != 'feat' else '') for f in dfpost.columns]),
                       'feat'
                ).filter(config.filterpairs
                ).drop('feat', 'df2'
                ).groupBy(*f2, struct('uid',
                                      struct(*[f for f in f1 if f != 'uid'])).alias('meta')
                ).agg(collect_list(struct('post2', 'df',
                                          array(struct('uid',
                                                       'post')).alias('alg'))).alias('post')
                ).filter(size('post') >= config.min_match)

    if config.all_pairs:
        apos = apos.withColumn('meta', map_from_entries(array('meta'))
                  ).withColumn('post', sort_array('post'))
    else:
        apos = apos.groupBy(*f2
                  ).agg(map_from_entries(collect_list('meta')).alias('meta'),
                        merge_posts(collect_list('post')).alias('post'))

    apos.write.mode('ignore').parquet(pairsFname)
    
    vit_src = udf(lambda post, prior: vitSrc(post, prior,
                                      config.n, config.gap, config.min_align),
                  'array<struct<uid: bigint, begin2: int, end2: int, begin: int, end: int, anchors: array<struct<pos2: int, pos: int>>>>')

    spark.read.load(pairsFname
        ).withColumn('prior', f.map_from_arrays(f.map_keys('meta'),
                                                f.array_repeat(lit(1.0), size('meta')))
        ).withColumn('src', vit_src('post', 'prior')
        ).write.mode('ignore').parquet(srcFname)

    srcmap = spark.read.load(srcFname)

    if config.link_model:
        src_stats = udf(lambda src, docs: srcStats(src, docs),
                        'array<struct<uid:bigint, stats: array<struct<label:int, weight:int>>>>')

        data = srcmap.select(*f2, 'meta',
                             explode(src_stats('src', f.map_keys('meta'))).alias('stats')
                    ).select(*f2, 'meta', col('stats.*')
                    ).select(*f2, 'uid', (col('meta')[col('uid')]).alias('info'),
                             explode('stats').alias('stats')
                    ).select(*f2, 'uid', col('info.*'), col('stats.*')
                    ).filter(col('weight') > 0)

        # data.write.save(featFname)

        stages = []
        if config.link_features:
            stages.append(SQLTransformer(statement=
                                         f'SELECT *, {config.link_features} FROM __THIS__'))

        stages.append(RFormula(formula=f'label ~ {config.link_model}'))
        stages.append(LogisticRegression(maxIter=10, regParam=0.1, weightCol='weight',
                                         standardization=False))
        pipeline = Pipeline(stages=stages)

        model = pipeline.fit(data)

        # model.save(os.path.join(config.outputPath, "model0"))

        v1 = udf(lambda v: float(v[1]), 'double')

        prior = model.transform(data).select('uid2', 'uid', v1('probability').alias('prob')
                    ).distinct(
                    ).groupBy('uid2'
                    ).agg(map_from_entries(collect_list(struct('uid', 'prob'))).alias('prior'))
        
        srcmap.drop('prior'
             ).join(prior, 'uid2'
             ).withColumn('src', vit_src('post', 'prior')
             ).write.mode('ignore').parquet(os.path.join(config.outputPath, 'src1.parquet'))

        exit(0)

    span_edge = udf(lambda src: spanEdge(src, 200), # config.gap
                    'array<struct<uid: bigint, left2: int, begin2: int, end2: int, right2: int,'
                    + ' left: int, begin: int, end: int, right: int, anchors: array<struct<pos2: int, pos: int>>>>')

    grab_spans = udf(lambda src, text: ((text[s.left:s.begin],
                                         text[s.begin:(s.end - config.n)],
                                         text[(s.end - config.n):s.right]) for s in src),
                     'array<struct<prefix: string, text: string, suffix: string>>')

    grab_spans2 = udf(lambda src, text: ((text[s.left2:s.begin2],
                                          text[s.begin2:(s.end2 - config.n)],
                                          text[(s.end2 - config.n):s.right2]) for s in src),
                     'array<struct<prefix2: string, text2: string, suffix2: string>>')

    anchor_align = udf(lambda s1, s2, side: anchorAlign(config, s1, s2, side),
                       'struct<s1: string, s2: string>')

    # We align edges independently, but we could also consider
    # aligning gaps between target spans jointly so that they don't
    # overlap.

    spanFields = ['left', 'begin', 'end', 'right']
    spanFields += [f + '2' for f in spanFields]

    srcmap.withColumn('src', span_edge('src')
         ).drop('post'
         ).join(termCorpus.select(col('uid').alias('uid2'), col(config.text).alias('text2')),
                'uid2'
         ).withColumn('text2', grab_spans2('src', 'text2')
         ).withColumn('src', arrays_zip('src', 'text2')
         ).drop('meta', 'text2'
         ).select(*f2, explode('src').alias('src')
         ).select(*f2, col('src.src.*'), col('src.text2.*')
         ).groupBy('uid'
         ).agg(collect_list(struct(*f2, *spanFields, 'prefix2', 'text2', 'suffix2', 'anchors')).alias('src')
         ).join(termCorpus.withColumnRenamed(config.text, 'text'), 'uid'
         ).withColumn('text', grab_spans('src', 'text')
         ).withColumn('src', arrays_zip('src', 'text')
         ).drop('text'
         ).select(*f1, explode('src').alias('src')
         ).select(*f1, col('src.src.*'), col('src.text.*') # HACK! explode prevents UDF inlining
         ).withColumn('lalg', explode(array(anchor_align('prefix', 'prefix2', lit('left'))))
         ).withColumn('ralg', explode(array(anchor_align('suffix', 'suffix2', lit('right'))))
         ).withColumn('text', f.concat(col('lalg.s1'), col('text'), col('ralg.s1'))
         ).withColumn('text2', f.concat(col('lalg.s2'), col('text2'), col('ralg.s2'))
         ).withColumn('begin', col('begin') - length('lalg.s1')
         ).withColumn('end', col('end') + length('ralg.s1') - config.n
         ).withColumn('begin2', col('begin2') - length('lalg.s2')
         ).withColumn('end2', col('end2') + length('ralg.s2') - config.n
         ).drop('lalg', 'ralg', 'prefix', 'prefix2', 'suffix', 'suffix2' # debug fields
         ).drop('left', 'right', 'left2', 'right2'
         ).write.mode('ignore').parquet(extentsFname)

    extents = spark.read.load(extentsFname)

    if config.linewise:
        chunk_align = udf(lambda begin, begin2, text, text2, anchors:
                          chunkAlign(config, begin, begin2, text, text2, anchors),
                          'struct<s1: string, s2: string>')
        extentSet = set(extents.columns).difference(['uid'])
        simpleFields = [f['name'] for f in json.loads(corpus.schema.json())['fields']
                        if (isinstance(f['type'], str) and f['name'] not in extentSet)]
        
        extents.withColumn('alg', chunk_align('begin', 'begin2', 'text', 'text2', 'anchors')
                ).drop('anchors', 'text', 'text2'
                ).join(corpus.select(*simpleFields), 'uid'
                ).join(corpus.selectExpr(*[f'{n} as {n}2' for n in simpleFields]), 'uid2'
                ).write.mode('ignore').parquet(psgFname)

        psg = spark.read.load(psgFname)
        
        target_lines = udf(lambda begin, begin2, alg: targetLines(begin, begin2, alg),
                           'array<struct<b: int, b2: int, alg: string, alg2: string, matches: int>>')
        lines = psg.withColumn('lines', target_lines('begin', 'begin2', 'alg')
                    ).drop('alg', 'text', 'text2', 'begin', 'end', 'begin2', 'end2')
        
        # lines.selectExpr(*[f for f in lines.columns if f != 'lines'], 'inline(lines)'
        #                  ).write.json(os.path.join(config.outputPath, 'pass.json'))
        lines.write.format(config.output_format).save(outFname)
        exit(0)

    spark.conf.set('spark.sql.shuffle.partitions', spark.sparkContext.defaultParallelism)
    spark.sparkContext.setCheckpointDir(os.path.join(config.outputPath, 'tmp'))

    # TODO: Investigate why connected component computation still runs when output exists.
    clusterExtents(config, extents).write.mode('ignore').parquet(clustersFname)

    spark.conf.set('spark.sql.shuffle.partitions', corpus.rdd.getNumPartitions() * 3)
    
    clusterJoin(config, spark.read.load(clustersFname),
                corpus).write.mode('ignore').format(config.output_format).save(outFname)

    spark.stop()

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])