import json, os, sys
from math import ceil, log, inf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, udf, struct, length, collect_list, collect_set, sort_array, when, expr, explode, slice, map_from_entries, flatten
from pyspark.sql.types import ArrayType, StringType, LongType

from dataclasses import dataclass

def vitSrc(gid2, pos, meta, minmatch):
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

    docs = dict()
    docs[0] = 0
    for p in pos:
        inseries = any([meta[m.uid].gid == gid2 for m in p.alg])
        for m in p.alg:
            if inseries and meta[m.uid].gid == gid2:
                docs[m.uid] = docs.get(m.uid, 0) + 1

    ## b70 docs: 21918104; 237921873725 characters
    N = 21918104
    n = 5
    pcopy = 0.8
    pcont = 0.999956
    V = 256
    lpcopy = log(pcopy)
    lpmiss = log(1 - pcopy) - log(2 * V)
    lpstop = log((1 + pcopy) / 2)
    lpcont = log(pcont)
    lpswitch = log(1 - pcont)

    bp = dict()
    prev = {0: Score(0, 0, 0.0)}
    pmax = 0
    pargmax = 0
    last = 0
    for p in pos:
        if not any([docs.get(m.uid, 0) >= minmatch for m in p.alg]):
            continue
        npos = p.post2 + n
        stride = npos - last
        ## Spans can only begin or end at matches
        same = prev[0]
        bg = same.score + stride * (lpcont + -log(V))
        prev[0] = Score(npos, 0, bg + log(p.df) - log(N))
        bp[BP(0, npos, 0)] = BP(0, same.pos2, 0)
        # print(prev[0], file=sys.stderr)
        for m in p.alg:
            if docs.get(m.uid, 0) >= minmatch:
                continue
            same = prev.get(m.uid, Score(0, 0, -inf))
            gap = min(0, m.post - same.pos)
            gap2 = min(0, p.post2 - same.pos2)
            # Show we treat gaps on the source side the same? What about retrogrades?
            overlap = min(gap, gap2)
            minsub = ceil(overlap / n)
            minerr = minsub + abs(gap2 - gap) #gap2 - overlap
            cont = same.score + gap2 * lpcont + minerr * lpmiss + (overlap - minsub) * lpcopy
            switch = bg + lpswitch + log(0.001)
            score = stride * (lpcont + lpcopy)
            if (m.post + n) < same.pos or (switch > cont and stride > n and p.post2 > n):
                score += switch
                bp[BP(m.uid, npos, m.post + n)] = BP(0, last, 0)
            else:
                score += cont
                bp[BP(m.uid, npos, m.post + n)] = BP(m.uid, same.pos2, same.pos)
            prev[m.uid] = Score(npos, m.post + n, score)
            stop = score + lpstop + lpswitch
            if stop > prev[0].score:
                prev[0] = Score(npos, 0, stop)
                # print("# Stop %d @ %d: " % (m.uid, npos) + str(prev), file=sys.stderr)
                bp[BP(0, npos, 0)] = BP(m.uid, npos, m.post + n)
        last = npos
        # print("%d: " % npos, prev, file=sys.stderr)
        #print(bp, file=sys.stderr)

    res = list()
    cur = BP(0, last, 0)
    while cur.pos2 > 0:
        res.append(str(cur))
        cur = bp[cur]
    res.reverse()
    return res

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: %s <input> <output>' % sys.argv[0], file=sys.stderr)
        exit(-1)
    spark = SparkSession.builder.appName('Passim Alignment').getOrCreate()

    minmatch = 5
    vit_src = udf(lambda gid2, post, meta: vitSrc(gid2, post, meta, minmatch),
                  ArrayType(StringType()))

    raw = spark.read.load(sys.argv[1]).drop('feat', 'df2')

    postFields = ['df', 'post', 'post2']
    docFields = [f for f in raw.columns if f not in postFields]
    f1 = [f for f in docFields if not f.endswith('2')]
    f2 = [f for f in docFields if f.endswith('2')]

    raw.groupBy(*docFields
      ).agg(collect_list(struct(*postFields)).alias('post')
      ).filter(size('post') >= 5
      ).withColumn('post', explode('post')
      ).select(*docFields, col('post.*')
      ).groupBy(*f2, 'post2', 'df'
      ).agg(collect_list(struct('uid', 'post')).alias('alg'),
            collect_set(struct('uid', struct(*[f for f in f1 if f != 'uid']))).alias('meta')
      ).groupBy(*f2
      ).agg(sort_array(collect_list(struct('post2', 'df', 'alg'))).alias('post'),
            map_from_entries(flatten(collect_set('meta'))).alias('meta')
      ).withColumn('src', vit_src('gid2', 'post', 'meta')
      ).write.save(sys.argv[2])

    spark.stop()
