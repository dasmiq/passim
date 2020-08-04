import argparse
import heapq as hq
import json, os, sys
from collections import Counter, deque
from math import ceil, log, inf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, udf, struct, length, collect_list, collect_set, sort_array, when, expr, explode, map_from_entries, flatten, xxhash64, lit, array, arrays_zip, concat

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

def vitSrc(pos, n, max_gap, min_align):
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
            switch = bg + lpswitch + log(0.001) + n * (lpcont + lpcopy)
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
                                          matches[i].pos, matches[j-1].pos + n))
                        break
                j += 1
            i = j
        i += 1

    return spans

def spanEdge(src, max_gap):
    res = list()
    for i, s in enumerate(src):
        lend = src[i-1].end2 if i > 0 else 0
        left2 = max(lend, s.begin2 - max_gap)
        rend = src[i+1].begin2 if (i + 1) < len(src) else s.end2 + max_gap
        right2 = min(rend, s.end2 + max_gap)
        left = max(0, s.begin - (s.begin2 - left2 + 20))
        right = s.end + 20 + (right2 - s.end2)
        res.append((s.uid, left2, s.begin2, s.end2, right2, left, s.begin, s.end, right))
    return res

def anchorAlign(s1, s2, side):
    if side == 'left':
        s1 = s1[::-1]
        s2 = s2[::-1]

    width = 10
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
                else:
                    cand.append((score - lpedit, (s + 1, t + 1, e1, e2)))
        if t < len(s2):         # insert
            cand.append((score - lpedit, (s, t + 1, e1, e1)))
        for c in cand:
            (score, item) = c
            if item[2] == 0 and abs(item[0] - item[1]) > width:
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

def main(config):
    spark = SparkSession.builder.appName('Passim Alignment').getOrCreate()

    dfpostFname = os.path.join(config.outputPath, 'dfpost.parquet')
    pairsFname = os.path.join(config.outputPath, 'pairs.parquet')
    srcFname = os.path.join(config.outputPath, 'src.parquet')
    extentsFname = os.path.join(config.outputPath, 'extents.parquet')
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

    count_sources = udf(lambda post: dict(Counter(item.uid for s in post for item in s.alg)),
                        'map<bigint, int>')

    apos = dfpost.join(dfpost.toDF(*[f + ('2' if f != 'feat' else '') for f in dfpost.columns]),
                       'feat'
                ).filter(config.filterpairs
                ).drop('feat', 'df2'
                ).groupBy(*f2, 'post2', 'df'
                ).agg(collect_list(struct(*f1, 'post')).alias('alg'))

    if 'gid' in f1:
        apos = apos.withColumn('alg', expr('CASE WHEN forall(alg, a -> a.gid <> gid2)' +
                                           ' THEN alg ELSE filter(alg, a -> a.gid = gid2) END'))

    apos.groupBy(*f2
       ).agg(collect_list(struct('post2', 'df', 'alg')).alias('post')
       ).withColumn('freq', count_sources('post')
       ).withColumn('post',
                    expr(f'''sort_array(filter(
                               transform(post,
                                 p -> struct(p.post2 as post2, p.df as df,
                                             filter(p.alg,
                                               a -> freq[a.uid] >= {config.min_match}) as alg)),
                               p -> size(p.alg) > 0))''')
       ).drop('freq'
       ).filter(size('post') > 0
       ).write.mode('ignore').parquet(pairsFname)
    
    pairs = spark.read.load(pairsFname)
    
    vit_src = udf(lambda post: vitSrc(post,
                                      config.n, config.gap, config.min_align),
                  'array<struct<uid: bigint, begin2: int, end2: int, begin: int, end: int>>')

    pairs.withColumn('src', vit_src('post')).write.mode('ignore').parquet(srcFname)

    srcmap = spark.read.load(srcFname)

    span_edge = udf(lambda src: spanEdge(src, 200), # config.gap
                    'array<struct<uid: bigint, left2: int, begin2: int, end2: int, right2: int,'
                    + ' left: int, begin: int, end: int, right: int>>')

    grab_spans = udf(lambda src, text: ((text[s.left:s.begin],
                                         text[s.begin:(s.end - config.n)],
                                         text[(s.end - config.n):s.right]) for s in src),
                     'array<struct<prefix: string, text: string, suffix: string>>')

    grab_spans2 = udf(lambda src, text: ((text[s.left2:s.begin2],
                                          text[s.begin2:(s.end2 - config.n)],
                                          text[(s.end2 - config.n):s.right2]) for s in src),
                     'array<struct<prefix2: string, text2: string, suffix2: string>>')

    anchor_align = udf(lambda s1, s2, side: anchorAlign(s1, s2, side),
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
         ).agg(collect_list(struct(*f2, *spanFields, 'prefix2', 'text2', 'suffix2')).alias('src')
         ).join(termCorpus.withColumnRenamed(config.text, 'text'), 'uid'
         ).withColumn('text', grab_spans('src', 'text')
         ).withColumn('src', arrays_zip('src', 'text')
         ).drop('text'
         ).select(*f1, explode('src').alias('src')
         ).select(*f1, col('src.src.*'), col('src.text.*') # HACK! explode prevents UDF inlining
         ).withColumn('lalg', explode(array(anchor_align('prefix', 'prefix2', lit('left'))))
         ).withColumn('ralg', explode(array(anchor_align('suffix', 'suffix2', lit('right'))))
         ).withColumn('text', concat(col('lalg.s1'), col('text'), col('ralg.s1'))
         ).withColumn('text2', concat(col('lalg.s2'), col('text2'), col('ralg.s2'))
         ).withColumn('begin', col('begin') - length('lalg.s1')
         ).withColumn('end', col('end') + length('ralg.s1') - config.n
         ).withColumn('begin2', col('begin2') - length('lalg.s2')
         ).withColumn('end2', col('end2') + length('ralg.s2') - config.n
         ).drop('lalg', 'ralg'
         ).write.mode('ignore').parquet(extentsFname)
         
    spark.stop()

if __name__ == '__main__':
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
    parser.add_argument('-a', '--min-align', type=int, default=50,
                         help='Minimum length of alignment', metavar='N')
    parser.add_argument('--fields', type=str, nargs='+', default=[],
                        help='List of fileds to index')
    parser.add_argument('-f', '--filterpairs', type=str, default='uid < uid2',
                        help='SQL constraint on posting pairs; default=uid < uid2')
    parser.add_argument('--input-format', type=str, default='json',
                        help='Input format')
    parser.add_argument('--output-format', type=str, default='json',
                        help='Output format')
    parser.add_argument('inputPath', metavar='<path>', help='input data')
    parser.add_argument('outputPath', metavar='<path>', help='output')
    config = parser.parse_args()

    print(config)

    main(config)
