import argparse
import json, os, sys
from math import ceil, log, inf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, udf, struct, length, collect_list, collect_set, sort_array, when, expr, explode, slice, map_from_entries, flatten
from pyspark.sql.types import *

from dataclasses import dataclass

def vitSrc(pos, meta, n):
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
    N = 21918104
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

    matches = list()
    cur = BP(0, last, 0)
    while cur.pos2 > 0:
        matches.append(cur)
        cur = bp[cur]
    matches.reverse()

    ## Merge matches into spans
    spans = list()
    i = 0
    while i < len(matches):
        if matches[i].lab != 0:
            start = matches[i]
            j = i
            while j < len(matches):
                if matches[j].lab == 0:
                    if ((j+1) < len(matches) and matches[j+1].lab == matches[i].lab and
                        ## Allow singleton retrograde alignments
                        (matches[j+1].pos >= matches[i].pos
                         or matches[j+1].pos2 - n - matches[j].pos2 < 20)):
                        1
                    else:
                        spans.append((matches[i].lab, matches[i].pos2 - n, matches[j-1].pos2,
                                      matches[i].pos - n, matches[j-1].pos))
                        break
                j += 1
            i = j
        i += 1

    return spans

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Passim Alignment')
    parser.add_argument('-m', '--min-match', type=int, metavar='N', default=5,
                        help='Minimum number of n-gram matches between documents')
    parser.add_argument('-n', '--n', type=int, metavar='n', default=5,
                        help='n-gram order')
    parser.add_argument('-f', '--filterpairs', type=str, default='gid < gid2',
                        help='SQL constraint on posting pairs; default=gid < gid2')
    parser.add_argument('inputPath', metavar='<path>', help='input data')
    parser.add_argument('outputPath', metavar='<path>', help='output')
    config = parser.parse_args()

    print(config)

    dfpostFname = os.path.join(config.outputPath, 'dfpost.parquet')
    srcFname = os.path.join(config.outputPath, 'src.parquet')

    spark = SparkSession.builder.appName('Passim Alignment').getOrCreate()

    vit_src = udf(lambda post, meta: vitSrc(post, meta, config.n),
                  ArrayType(StructType([
                      StructField('uid', LongType()),
                      StructField('begin2', IntegerType()),
                      StructField('end2', IntegerType()),
                      StructField('begin', IntegerType()),
                      StructField('end', IntegerType())])))

    raw = spark.read.load(config.inputPath).drop('feat', 'df2')

    dfpost = spark.read.load(dfpostFname)

    spark.conf.set('spark.sql.shuffle.partitions', dfpost.rdd.getNumPartitions())

    raw = dfpost.join(dfpost.toDF(*[f + ('2' if f != 'feat' else '') for f in dfpost.columns]),
                      'feat'
               ).filter(config.filterpairs).drop('feat', 'df2')

    postFields = ['df', 'post', 'post2']
    docFields = [f for f in raw.columns if f not in postFields]
    f1 = [f for f in docFields if not f.endswith('2')]
    f2 = [f for f in docFields if f.endswith('2')]

    raw.groupBy(*docFields
      ).agg(collect_list(struct(*postFields)).alias('post')
      ).filter(size('post') >= config.min_match
      ).withColumn('post', explode('post')
      ).select(*docFields, col('post.*')
      ).groupBy(*f2, 'post2', 'df'
      ).agg(collect_list(struct('uid', 'post')).alias('alg'),
            collect_set(struct('uid', struct(*[f for f in f1 if f != 'uid']))).alias('meta')
      ).groupBy(*f2
      ).agg(sort_array(collect_list(struct('post2', 'df', 'alg'))).alias('post'),
            map_from_entries(flatten(collect_set('meta'))).alias('meta')
      ).withColumn('src', vit_src('post', 'meta')
      ).write.parquet(srcFname)

    spark.stop()
