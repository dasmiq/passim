import json, os, sys
from math import ceil, log, inf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, udf, struct, length, collect_list, sort_array, when, expr, explode, slice
from pyspark.sql.types import ArrayType, StringType, LongType

from dataclasses import dataclass

def vitSrc(pos):
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
        for m in p.alg:
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
        npos = p.post2 + n
        stride = npos - last
        ## Spans can only begin or end at matches
        same = prev[0]
        bg = same.score + stride * (lpcont + -log(V))
        prev[0] = Score(npos, 0, bg + log(p.df) - log(N))
        bp[BP(0, npos, 0)] = BP(0, same.pos2, 0)
        # print(prev[0], file=sys.stderr)
        for m in p.alg:
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

    vit_src = udf(lambda pos: vitSrc(pos), ArrayType(StringType()))

    raw = spark.read.load(sys.argv[1])

    
    goods = raw.join(raw.filter(col('gid') == col('gid2')).groupBy('uid2', 'post2').count(),
                     ['uid2', 'post2'], 'leftouter'
               ).filter(col('count').isNull() | (col('gid') == col('gid2'))
               ).groupBy('uid', 'uid2').count().filter(col('count') >= 5)

    raw.join(goods, ['uid', 'uid2'], 'leftsemi'
      ).groupBy('uid2', 'gid2', 'post2', 'df'
      ).agg(collect_list(struct('uid', 'gid', 'post')).alias('alg')
      ).groupBy('uid2', 'gid2'
      ).agg(sort_array(collect_list(struct('post2', 'df', 'alg'))).alias('post')
      ).withColumn('src', vit_src('post')
      ).write.json(sys.argv[2])

    spark.stop()
