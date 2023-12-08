import argparse
import heapq as hq
import json, os, sys
from collections import deque
from intervaltree import Interval, IntervalTree
from math import ceil, log, inf
from pyspark.sql import SparkSession, Row, DataFrame
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

def spliceExtents(extents):
    res = list()
    cur = None
    for ex in extents:
        if cur == None:
            cur = ex
        elif ex.begin2 < cur.end2 < ex.end2 and cur.begin < ex.begin < cur.end < ex.end:
            cur = Row(begin2=cur.begin2, end2=ex.end2,
                      begin=cur.begin, end=ex.end,
                      text2=(cur.text2 + ex.text2[(cur.end2 - ex.begin2):]),
                      text=(cur.text + ex.text[(cur.end - ex.begin):]),
                      anchors=(cur.anchors + ex.anchors))
        else:
            res.append(cur)
            cur = ex
    if cur != None:
        res.append(cur)
    return res

def countMatches(s1, s2):
    return len(list(filter(lambda c: (c[0].lower() == c[1].lower() or
                                      (c[0].isspace() and c[1].isspace())),
                           zip(s1, s2))))

def beamAnchorAlign(s1, s2, side,
                    pcopy=0.8, beam=20, complete_lines=False, floating_ngrams=False):
    if side == 'left':
        s1 = s1[::-1]
        s2 = s2[::-1]

    V = 256
    logV = log(V)
    lpcopy = log(pcopy)
    lpedit = log(1 - pcopy) - log(2 * V)
    lpstop = log((1 + pcopy) / 2)
    pfinal = 0.01
    lpfinal = log(pfinal)
    lppad = log(1 - pfinal) - log(V)

    # Progress by target text position
    t = 0
    best = (0, 0)
    bestScore = float('inf')
    # Items are (score, s)
    cur = [(0, 0)]
    suc = []
    while t <= len(s2):
        # print(t)
        pops = 0
        seen = {}
        while len(cur) > 0 and pops < beam:
            top = hq.heappop(cur)
            (score, s) = top
            if score >= bestScore or (s in seen and score >= seen[s]):
                continue
            seen[s] = score
            pops += 1
            # print(top)
            ## Finish
            if t == len(s2) or s2[t] == '\n' or ((not complete_lines) and (floating_ngrams or (not s2[t].isalnum()))):
                finalScore = score - (lpstop + 2 * lpfinal + lppad * (len(s2) - t))
                if finalScore < bestScore:
                    bestScore = finalScore
                    best = (s, t)
            if s < len(s1):     # delete
                hq.heappush(cur, (score - lpedit, s + 1))
                if t < len(s2):
                    if s1[s].lower() == s2[t].lower() or (s1[s].isspace() and s2[t].isspace()): #copy
                        hq.heappush(suc, (score - lpcopy, s + 1))
                    elif s1[s] != '\n' and s2[t] != '\n': # newlines can't rewrite as printable
                        hq.heappush(suc, (score - lpedit, s + 1))
            if t < len(s2):     # insert
                hq.heappush(suc, (score - lpedit, s))

        t += 1
        cur = suc
        suc = []

    (e1, e2) = best
    # print("# BEST!")
    # print(best)
    # print(bestScore)
    
    # It might be worth deleting small numbers of characters trailing (leading) a newline.
    if side == 'left':
        return (s1[e1-1::-1], s2[e2-1::-1])
    else:
        return (s1[0:e1], s2[0:e2])

def anchorAlign(s1, s2, side,
                pcopy=0.8, max_offset=20, complete_lines=False, floating_ngrams=False):
    if side == 'left':
        s1 = s1[::-1]
        s2 = s2[::-1]

    V = 256
    logV = log(V)
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
        cand = []
        ## Finish
        if t == len(s2) or s2[t] == '\n' or ((not config.complete_lines) and (config.floating_ngrams or (not s2[t].isalnum()))):
            cand.append((score - (lpstop + 2 * lpfinal + lppad * (len(s2) - t)),
                         (len(s1), len(s2), s, t)))
        if s < len(s1):         # delete
            cand.append((score - lpedit, (s + 1, t, e1, e2)))
            if t < len(s2):
                if s1[s].lower() == s2[t].lower() or (s1[s].isspace() and s2[t].isspace()): #copy
                    cand.append((score - lpcopy, (s + 1, t + 1, e1, e2)))
                elif s1[s] != '\n' and s2[t] != '\n': # newlines can't rewrite as printable
                    cand.append((score - lpedit, (s + 1, t + 1, e1, e2)))
        if t < len(s2):         # insert
            cand.append((score - lpedit, (s, t + 1, e1, e2)))
        for c in cand:
            (score, item) = c
            if item[2] == 0 and abs(item[0] - item[1]) > max_offset:
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

def levAlign(s1, s2, pcopy=0.8, beam=20, max_offset=20):
    V = 256
    logV = log(V)
    lpcopy = log(pcopy)
    lpedit = log(1 - pcopy) - log(2 * V)

    s1 = s1.replace('-', '\u2010')
    s2 = s2.replace('-', '\u2010')
    if len(s1) == len(s2) and s1 == s2: # There's probably a length break-even point here.
        return (s1, s2)
    elif len(s1) == 0:
        return ('-' * len(s2), s2)
    elif len(s2) == 0:
        return (s1, '-' * len(s1))

    if beam > 0:
        t = 0
        bestScore = float('inf')
        best = (0, '', '')
        cur = [(0, (0, '', ''))] # items are (source, alg1, alg2)
        suc = []
        while t <= len(s2):
            # print(t)
            pops = 0
            seen = {}
            while len(cur) > 0 and pops < beam:
                top = hq.heappop(cur)
                (score, (s, a1, a2)) = top
                if s == len(s1) and t == len(s2) and score <= bestScore:
                    bestScore = score
                    best = (s, a1, a2)
                if s in seen and score >= seen[s]:
                    continue
                seen[s] = score
                pops += 1
                # print(top)
                if s < len(s1): # delete
                    hq.heappush(cur, (score - lpedit, (s + 1, a1 + s1[s], a2 + '-')))
                    if t < len(s2):
                        if s1[s] == s2[t] or (s1[s].isspace() and s2[t].isspace()): #copy
                            hq.heappush(suc, (score - lpcopy, (s + 1, a1+s1[s], a2+s2[t])))
                        elif s1[s] != '\n' and s2[t] != '\n':
                            hq.heappush(suc, (score - lpedit, (s + 1, a1+s1[s], a2+s2[t])))
                if t < len(s2): # insert
                    hq.heappush(suc, (score - lpedit, (s, a1 + '-', a2 + s2[t])))
            t += 1
            cur = suc
            suc = []

        if bestScore < inf:
            (s, a1, a2) = best
            return (a1, a2)
        else:                       # alignment failed
            return (s1 + '-' * max(0, len(s2) - len(s1)),
                    s2 + '-' * max(0, len(s1) - len(s2)))            
    else:
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
            # Option to bail out
            cand = list()
            if e1 < len(s1):         # delete
                cand.append((score - lpedit, (e1 + 1, e2)))
                if e2 < len(s2):
                    if s1[e1] == s2[e2] or (s1[e1].isspace() and s2[e2].isspace()): #copy
                        cand.append((score - lpcopy, (e1 + 1, e2 + 1)))
                    elif s1[e1] != '\n' and s2[e2] != '\n':
                        cand.append((score - lpedit, (e1 + 1, e2 + 1)))
            if e2 < len(s2):         # insert
                cand.append((score - lpedit, (e1, e2 + 1)))
            for c in cand:
                (score, item) = c
                if (abs(item[0]/len(s1) - item[1]/len(s2))*len(s1)) > max_offset:
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
            return (s1 + '-' * max(0, len(s2) - len(s1)),
                    s2 + '-' * max(0, len(s1) - len(s2)))

def chunkAlign(begin, begin2, text, text2, anchors, pcopy, beam, max_offset):
    alg1 = ''
    alg2 = ''
    b1 = 0
    b2 = 0
    for a in anchors:
        e1 = a.pos - begin
        e2 = a.pos2 - begin2
        (s1, s2) = levAlign(text[b1:e1], text2[b2:e2], pcopy, beam, max_offset)
        alg1 += s1
        alg2 += s2
        b1 = e1
        b2 = e2
    (s1, s2) = levAlign(text[b1:len(text)], text2[b2:len(text2)], pcopy, beam, max_offset)
    alg1 += s1
    alg2 += s2
    return (alg1, alg2, countMatches(alg1, alg2))

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

def textLines(text):
    lines = list()
    off = 0
    for line in text.splitlines(keepends=True):
        lines.append((off, line))
        off += len(line)
    return lines

def loverlap(s1, s2):
    return max(0, min(s1.end, s2.end) - max(s1.begin, s2.begin))

def linkSpans(src_overlap, dst_overlap, s1, s2):
    """Link pairs of spans within the same document, such that either:
    1. one span is the source for a link to another document and the
       other span is the destination for a link from another document; or
    2. two spans that are not desintations overlap with each other.
    """
    src = IntervalTree([Interval(s[0], s[1]) for s in s2]) if s2 != None else IntervalTree()
    res = list()
    unsourced = list()
    for s in s1:
        cur = Interval(s[0], s[1])
        sources = [i for i in src[cur.begin:cur.end]
                   if loverlap(cur, i) / cur.length() >= dst_overlap]
        # NB: There shouldn't be much overlap in spans that are the
        # destinations of links from other documents (i.e., sources
        # here), except due to edge alignments; nevertheless, pick the
        # largest overlap.
        sources.sort(key=lambda i: loverlap(cur, i) / cur.length(), reverse=True)
        if sources:
            res.append((cur.begin, cur.end, sources[0].begin, sources[0].end))
        else:
            unsourced.append(cur)
    if len(unsourced) > 0:
        unsourced.sort(key=lambda i: (-i.length(), i.begin, i.end))
        for i in range(1, len(unsourced)):
            dst = unsourced[i]
            for j in range(i-1, -1, -1):
                src = unsourced[j]
                if (loverlap(dst, src) / dst.length()) >= src_overlap:
                    res.append((dst.begin, dst.end, src.begin, src.end))
                    break
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

def passLocs(self, corpus, locsCol, newCol, uidCol, begin, end):
    if locsCol not in corpus.columns:
        return self

    pinfo = corpus.select('uid', explode(locsCol).alias('locs')
                ).select('uid', 'locs.*')

    return self.join(pinfo, [self[uidCol] == pinfo.uid,
                             end > pinfo.start,
                             begin < (pinfo.start + pinfo.length)],
                     'left_outer'
            ).drop(pinfo.uid
            ).groupBy(*self.columns
            ).agg(sort_array(collect_list('loc')).alias('locs'))
    
def passRegions(self, corpus, pageCol, newCol, uidCol, begin, end, keeptokens=False):
    if pageCol not in corpus.columns:
        return self
    pageFields = [f for f
                  in corpus.selectExpr(f'inline({pageCol})').columns if f != 'regions']
    pfList = ', '.join([f'page.{f} as {f}' for f in pageFields])

    pinfo = corpus.select('uid', explode(pageCol).alias('page')
                ).select('uid', expr(f'struct({pfList}) as page'),
                         expr('inline(page.regions)'))
    res = self.join(pinfo, [self[uidCol] == pinfo.uid,
                            begin <= pinfo.start,
                            end >= (pinfo.start + pinfo.length)],
                    'left_outer'
            ).drop(pinfo.uid
            ).groupBy(*self.columns, 'page'
            ).agg(when(col('page').isNotNull(),
                       array(struct(*[f'page.{f}' for f in pageFields],
                               sort_array(collect_list(struct('start', 'length', 'coords'))).alias('regions')))).alias(newCol + 'Tokens'),
                  when(col('page').isNotNull(),
                       array(struct(*[f'page.{f}' for f in pageFields],
                               array(struct(f.min('start').alias('start'),
                                            (f.max(col('start') + col('length')) - f.min('start')).alias('length'),
                                            struct(f.min('coords.x').alias('x'),
                                                   f.min('coords.y').alias('y'),
                                                   (f.max(col('coords.x') + col('coords.w')) - f.min('coords.x')).alias('w'),
                                                   (f.max(col('coords.y') + col('coords.h')) - f.min('coords.y')).alias('h')
                                                   
                                                   ).alias('coords'))).alias('regions'))
                        )).alias(newCol)
            ).drop('page')
    if not keeptokens:
        res = res.drop(newCol + 'Tokens')
    return res

setattr(DataFrame, 'passLocs', passLocs)
setattr(DataFrame, 'passRegions', passRegions)

def clusterExtents(self, config):
    s1 = self.groupBy('uid').agg(sort_array(collect_set(struct('begin', 'end'))).alias('s1'))
    s2 = self.groupBy('uid2'
               ).agg(sort_array(collect_set(struct('begin2', 'end2'))).alias('s2'))

    link_spans = udf(lambda s1, s2: linkSpans(config.src_overlap, config.dst_overlap, s1, s2),
                     'array<struct<begin: int, end: int, begin2: int, end2: int>>')

    within = s1.join(s2, col('uid') == col('uid2'), 'leftouter'
              ).select('uid', explode(link_spans('s1', 's2')).alias('link')
              ).selectExpr('struct(uid, link.begin2 as begin, link.end2 as end) as src',
                           'struct(uid, link.begin, link.end) as dst',
                           'false as boiler'
              ).distinct()

    boiler = 'gid = gid2' if 'gid' in self.columns else 'false'

    between = self.selectExpr('struct(uid, begin, end) as src',
                              'struct(uid2 as uid, begin2 as begin, end2 as end) as dst',
                              f'{boiler} as boiler')

    vert = self.selectExpr('struct(uid2 as uid, begin2 as begin, end2 as end) as id',
                ).union(self.selectExpr('struct(uid, begin, end) as id')
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
    
setattr(DataFrame, 'clusterExtents', clusterExtents)

def clusterJoin(self, config, corpus):
    out = self.join(corpus, 'uid'
            ).withColumn(config.text,
                         col(config.text).substr(col('begin'), col('end') - col('begin')))

    if config.locs in out.columns:
        out = out.withColumn(config.locs,
                             expr(f"filter({config.locs}, r -> r.start < end AND (r.start + r.length) > begin)['loc']"))

    pageCol = config.pages
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

setattr(DataFrame, 'clusterJoin', clusterJoin)

def main(args):
    parser = argparse.ArgumentParser(description='Passim Alignment',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-i', '--id', type=str, default='id',
                        help='Field for unique document IDs')
    parser.add_argument('-t', '--text', type=str, default='text',
                        help='Field for document text')
    parser.add_argument('--locs', type=str, default='locs',
                        help='Field for citable loci')
    parser.add_argument('--pages', type=str, default='pages',
                        help='Field for page location information')
    parser.add_argument('-l', '--minDF', type=int, default=2,
                        help='Lower limit on document frequency', metavar='N')
    parser.add_argument('-u', '--maxDF', type=int, default=100,
                        help='Upper limit on document frequency', metavar='N')
    parser.add_argument('-m', '--min-match', type=int, metavar='N', default=5,
                        help='Minimum number of n-gram matches between documents')
    parser.add_argument('-n', '--n', type=int, default=25,
                        help='n-gram order', metavar='N')
    parser.add_argument('--floating-ngrams', action='store_true',
                        help='Allow n-grams to float from word boundaries')
    parser.add_argument('--complete-lines', action='store_true',
                        help='Break target alignments at line breaks')
    parser.add_argument('-g', '--gap', type=int, default=600,
                        help='Minimum size of gap that separates passages', metavar='N')
    parser.add_argument('--max-offset', type=int, default=20,
                        help='Maximum offset in global alignment [deprecated]', metavar='N')
    parser.add_argument('--beam', type=int, default=20,
                        help='Beam search width', metavar='N')
    parser.add_argument('--pcopy', type=float, default=0.8,
                        help='Probability of copying a character', metavar='p')
    parser.add_argument('-a', '--min-align', type=int, default=50,
                         help='Minimum length of alignment', metavar='N')
    parser.add_argument('--src-overlap', type=float, default=0.9,
                        help='Source overlap proportion', metavar='p')
    parser.add_argument('--dst-overlap', type=float, default=0.5,
                        help='Destination overlap proportion', metavar='p')
    parser.add_argument('--fields', type=str, nargs='+', default=[],
                        help='List of fields to index')
    parser.add_argument('-f', '--filterpairs', type=str, default='uid < uid2',
                        help='SQL constraint on posting pairs')
    parser.add_argument('--all-pairs', action='store_true',
                        help='Compute alignments for all pairs.')
    parser.add_argument('--pairwise', action='store_true', help='Output pairwise alignments')
    parser.add_argument('--docwise', action='store_true', help='Output docwise alignments')
    parser.add_argument('--linewise', action='store_true', help='Output linewise alignments')
    parser.add_argument('--to-pairs', action='store_true', help='Output pairs and stop')    
    parser.add_argument('--to-extents', action='store_true', help='Output extents and stop')    
    parser.add_argument('--link-model', type=str, default=None,
                        help='Link model in R format')
    parser.add_argument('--link-features', type=str, default=None,
                        help='Link model features as SQL SELECT')
    parser.add_argument('--log-level', type=str, default='WARN',
                        choices=['ERROR', 'WARN', 'INFO', 'DEBUG'],
                        help='spark log level')
    parser.add_argument('--input-format', type=str, default='json',
                        help='Input format')
    parser.add_argument('--output-format', type=str, default='json',
                        help='Output format')
    parser.add_argument('inputPath', metavar='<path>', help='input data')
    parser.add_argument('outputPath', metavar='<path>', help='output')
    config = parser.parse_args(args)

    print(config)

    spark = SparkSession.builder.appName('Passim Alignment').getOrCreate()
    spark.sparkContext.setLogLevel(config.log_level)
    spark.conf.set('spark.sql.parquet.datetimeRebaseModeInRead', 'CORRECTED')
    spark.conf.set('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED')

    tmpdir = 'tmp'
    spark.sparkContext.setCheckpointDir(os.path.join(config.outputPath, tmpdir))
    
    dfpostFname = os.path.join(config.outputPath, tmpdir, 'dfpost.parquet')
    pairsFname = os.path.join(config.outputPath, tmpdir, 'pairs.parquet')
    srcFname = os.path.join(config.outputPath, tmpdir, 'src.parquet')
    featFname = os.path.join(config.outputPath, tmpdir, 'feat.parquet')
    extentsFname = os.path.join(config.outputPath, tmpdir, 'extents.parquet')
    psgFname = os.path.join(config.outputPath, tmpdir, 'psg.parquet')
    clustersFname = os.path.join(config.outputPath, tmpdir, 'clusters.parquet')
    alignFname = os.path.join(config.outputPath, 'align.' + config.output_format)
    outFname = os.path.join(config.outputPath, 'out.' + config.output_format)

    corpus = spark.read.option('mergeSchema',
                               'true').format(config.input_format).load(config.inputPath
                               ).na.drop(subset=[config.id, config.text]
                               ).withColumn('uid', xxhash64(config.id))

    termCorpus = corpus.selectExpr('uid', config.text, *config.fields)
    f1 = [f for f in termCorpus.columns if f != config.text]
    f2 = [f + '2' for f in f1]

    spark.conf.set('spark.sql.shuffle.partitions', corpus.rdd.getNumPartitions())

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

    metaFields = ', '.join([f for f in f1 if f != 'uid'])
    if metaFields != '':
        metaVal = f'struct({metaFields})'
    else:
        metaVal = 'struct(1 AS meta)'

    apos = dfpost.join(dfpost.toDF(*[f + ('2' if f != 'feat' else '') for f in dfpost.columns]),
                       'feat'
                ).filter(config.filterpairs
                ).drop('feat', 'df2'
                ).groupBy(*f2, struct('uid', expr(metaVal)).alias('meta')
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
    if config.to_pairs:
        spark.stop()
        return(0)
    
    vit_src = udf(lambda post, prior: vitSrc(post, prior,
                                      config.n, config.gap, config.min_align),
                  'array<struct<uid: bigint, begin2: int, end2: int, begin: int, end: int, anchors: array<struct<pos2: int, pos: int>>>>')

    spark.read.load(pairsFname
        ).withColumn('prior', map_from_entries(arrays_zip(f.map_keys('meta'),
                                                          f.array_repeat(lit(1.0),size('meta'))))
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

        spark.stop()
        return(0)

    span_edge = udf(lambda src: spanEdge(src, config.gap),
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

    anchor_align = udf(lambda s1, s2, side: anchorAlign(s1, s2, side, config.pcopy, config.max_offset, config.complete_lines, config.floating_ngrams),
                       'struct<s1: string, s2: string>').asNondeterministic() \
                       if config.beam == 0 \
                          else udf(lambda s1, s2, side: beamAnchorAlign(s1, s2, side, config.pcopy, config.beam, config.complete_lines, config.floating_ngrams),
                                   'struct<s1: string, s2: string>').asNondeterministic()

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
         ).select(*f1, col('src.src.*'), col('src.text.*')
         ).withColumn('lalg', anchor_align('prefix', 'prefix2', lit('left'))
         ).withColumn('ralg', anchor_align('suffix', 'suffix2', lit('right'))
         ).withColumn('text', f.concat(col('lalg.s1'), col('text'), col('ralg.s1'))
         ).withColumn('text2', f.concat(col('lalg.s2'), col('text2'), col('ralg.s2'))
         ).withColumn('begin', col('begin') - length('lalg.s1')
         ).withColumn('end', col('end') + length('ralg.s1') - config.n
         ).withColumn('begin2', col('begin2') - length('lalg.s2')
         ).withColumn('end2', col('end2') + length('ralg.s2') - config.n
         ).drop('lalg', 'ralg', 'prefix', 'prefix2', 'suffix', 'suffix2' # debug fields
         ).drop('left', 'right', 'left2', 'right2'
         ).write.mode('ignore').parquet(extentsFname)

    if config.to_extents:
        spark.stop()
        return(0)

    splice_extents = udf(lambda extents: spliceExtents(extents),
                         'array<struct<begin2: int, end2: int, begin: int, end: int, text2: string, text: string, anchors: array<struct<pos2: int, pos: int>>>>')

    extents = spark.read.load(extentsFname
         ).groupBy(*f1, *f2
         ).agg(splice_extents(sort_array(collect_list(struct('begin2', 'end2', 'begin', 'end',
                                                             'text2', 'text', 'anchors')))
                              ).alias('extents')
         ).withColumn('extents', explode('extents')
         ).select(*f1, *f2, col('extents.*'))

    if config.pairwise or config.docwise or config.linewise:
        chunk_align = udf(lambda begin, begin2, text, text2, anchors:
                          chunkAlign(begin, begin2, text, text2, anchors,
                                     config.pcopy, config.beam, config.max_offset),
                          'struct<s1: string, s2: string, matches: int>')
        extentSet = set(extents.columns).difference(['uid'])
        simpleFields = [f['name'] for f in json.loads(corpus.schema.json())['fields']
                        if (isinstance(f['type'], str) and f['name'] not in extentSet)]
        
        extents.withColumn('alg', chunk_align('begin', 'begin2', 'text', 'text2', 'anchors')
            ).drop('anchors', 'text', 'text2'
            ).write.mode('ignore').parquet(psgFname)

        psg = spark.read.load(psgFname)

        passalg = psg.join(corpus.select(*simpleFields), 'uid'
                    ).join(corpus.selectExpr(*[f'{n} as {n}2' for n in simpleFields]), 'uid2')

        if config.pairwise:
            passalg.select('*', 'alg.*'
                    ).drop('alg'
                    ).passRegions(corpus, config.pages, config.pages,
                                  'uid', col('begin'), col('end')
                    ).passRegions(corpus, config.pages, config.pages + '2',
                                  'uid2', col('begin2'), col('end2')
                    ).write.mode('ignore').format(config.output_format).save(alignFname)

        if config.docwise or config.linewise:
            target_lines = udf(lambda begin, begin2, alg: targetLines(begin, begin2, alg),
                               'array<struct<begin: int, begin2: int, alg: string, alg2: string, matches: int>>')
            groups = passalg.withColumn('lines', target_lines('begin', 'begin2', 'alg')
                            ).drop('alg', 'text', 'text2', 'begin', 'end', 'begin2', 'end2')
            lines = groups.selectExpr(*[f for f in groups.columns if f != 'lines'], 'inline(lines)'
                        ).withColumn('text', f.translate('alg', '-', '')
                        ).withColumn('text2', f.translate('alg2', '-', '')
                        ).passLocs(corpus, config.locs, config.locs,
                                   'uid', col('begin'), col('begin') + length('text'))

            if config.docwise:
                text_lines = udf(lambda text: textLines(text),
                                 'array<struct<begin: int, text: string>>')
                lines.groupBy('uid2', 'begin2'
                    ).agg(collect_list(struct(*[f for f in lines.columns
                                                if ( (not f.endswith('2') or f == 'alg2')
                                                     and f != 'uid')])).alias('wits')
                    ).groupBy(col('uid2').alias('uid')
                    ).agg(collect_list(struct(col('begin2').alias('begin'),
                                              col('wits'))).alias('vars')
                    ).withColumn('vars', map_from_entries(arrays_zip(col('vars.begin'),
                                                                     col('vars.wits')))
                    ).join(corpus, 'uid'
                    ).withColumn('lines', text_lines('text')
                    ).withColumn('lines',
                                 expr('transform(lines, r -> struct(r.begin as begin, r.text as text, vars[r.begin] as wits))')
                    ).drop('vars'
                    ).write.mode('ignore').format(config.output_format).save(outFname)
                spark.stop()
                return

            lines.passRegions(corpus, config.pages, config.pages,
                              'uid', col('begin'), col('begin') + length('text'), True
                ).passRegions(corpus, config.oages, config.pages + '2',
                              'uid2', col('begin2'), col('begin2') + length('text2'), True
                ).write.mode('ignore').format(config.output_format).save(outFname)
            spark.stop()
            return

    if not os.path.exists(clustersFname): # prevent creating tmpdirs in clustering 
        spark.conf.set('spark.sql.shuffle.partitions', spark.sparkContext.defaultParallelism)
        extents.clusterExtents(config).write.mode('ignore').parquet(clustersFname)
        spark.conf.set('spark.sql.shuffle.partitions', corpus.rdd.getNumPartitions())
    
    spark.read.load(clustersFname
        ).clusterJoin(config, corpus
        ).write.mode('ignore').format(config.output_format).save(outFname)

    spark.stop()
    return(0)

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
