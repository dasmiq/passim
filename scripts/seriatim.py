import argparse
import json, os, sys
from math import ceil, log, inf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode, size, udf, struct, length, collect_list, collect_set, sort_array, when, expr, explode, slice, map_from_entries, flatten, xxhash64, monotonically_increasing_id, lit, array, arrays_zip
from pyspark.sql.types import *

from dataclasses import dataclass

def getPostings(text, n, floating_ngrams):
    tf = dict()
    posts = list()
    for i, c in enumerate(text):
        if c.isalnum() and ( floating_ngrams or i == 0 or not text[i-1].isalnum() ):
            j = i + 1
            buf = ''
            while j < len(text) and len(buf) < n:
                if text[j].isalnum():
                    buf += text[j].lower()
                j += 1
            if len(buf) >= n:
                tf[buf] = tf.get(buf, 0) + 1
                posts.append((buf, i))
    return [(key, tf[key], i) for key, i in posts]

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
    pcont = 0.98
    V = 256
    lpcopy = log(pcopy)
    lpmiss = log(1 - pcopy) - log(2 * V)
    lpstop = log((1 + pcopy) / 2)
    lpcont = log(pcont)
    lpswitch = log(1 - pcont)

    ## Should probably just use start points in dynamic program
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
            gap = max(0, m.post - same.pos)
            gap2 = max(0, p.post2 - same.pos2)
            # Show we treat gaps on the source side the same? What about retrogrades?
            overlap = min(gap, gap2)
            minsub = ceil(overlap / n)
            minerr = minsub + abs(gap2 - gap) #gap2 - overlap
            cont = same.score + gap2 * lpcont + minerr * lpmiss + (overlap - minsub) * lpcopy
            switch = bg + lpswitch + log(0.001)
            score = stride * (lpcont + lpcopy)
            if gap2 > max_gap or (m.post + n) < same.pos or (switch > cont and stride > n and p.post2 > n):
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

    # return [(m.lab, m.pos2 - n, m.pos2, m.pos - n, m.pos) for m in matches]

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
                         or matches[j+1].pos2 - n - matches[j].pos2 < 20)):
                        1
                    else:
                        if (matches[j-1].pos2 - matches[i].pos2 + n) >= min_align:
                            spans.append((matches[i].lab, matches[i].pos2 - n, matches[j-1].pos2,
                                          matches[i].pos - n, matches[j-1].pos))
                        break
                j += 1
            i = j
        i += 1

    return spans

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

    spark = SparkSession.builder.appName('Passim Alignment').getOrCreate()

    dfpostFname = os.path.join(config.outputPath, 'dfpost.parquet')
    srcFname = os.path.join(config.outputPath, 'src.parquet')
    outFname = os.path.join(config.outputPath, 'out.' + config.output_format)

    corpus = spark.read.option('mergeSchema',
                               'true').format(config.input_format).load(config.inputPath
                               ).na.drop(subset=[config.id, config.text]
                               ).withColumn('uid', xxhash64(config.id))

    termCorpus = corpus.selectExpr('uid', config.text, *config.fields)

    spark.conf.set('spark.sql.shuffle.partitions', corpus.rdd.getNumPartitions() * 3)

    get_postings = udf(lambda text: getPostings(text, config.n, config.floating_ngrams),
                       ArrayType(StructType([
                           StructField('feat', StringType()),
                           StructField('tf', IntegerType()),
                           StructField('post', IntegerType())])))

    posts = termCorpus.withColumn('post', explode(get_postings(config.text))
                     ).select(*[col(f) for f in termCorpus.columns], col('post.*')
                     ).drop(config.text
                     ).withColumn('feat', xxhash64('feat')
                     ).filter(col('tf') == 1
                     ).drop('tf')

    df = posts.groupBy('feat').count().select('feat', col('count').cast('int').alias('df')
             ).filter( (col('df') >= config.minDF) & (col('df') <= config.maxDF) )

    posts.join(df, 'feat').write.mode('ignore').save(dfpostFname)
    
    vit_src = udf(lambda post: vitSrc(post, config.n, config.gap, config.min_align),
                  ArrayType(StructType([
                      StructField('uid', LongType()),
                      StructField('begin2', IntegerType()),
                      StructField('end2', IntegerType()),
                      StructField('begin', IntegerType()),
                      StructField('end', IntegerType())])))

    dfpost = spark.read.load(dfpostFname)

    f1 = [f for f in dfpost.columns if f not in ['feat', 'df', 'post']]
    f2 = [f + '2' for f in f1]

    spark.conf.set('spark.sql.mapKeyDedupPolicy', 'LAST_WIN')
    
    dfpost.join(dfpost.toDF(*[f + ('2' if f != 'feat' else '') for f in dfpost.columns]),
                'feat'
         ).filter(config.filterpairs
         ).drop('feat', 'df2'
         ).groupBy(*f1, *f2
         ).agg(collect_list(struct('df', 'post', 'post2')).alias('post')
         ).filter(size('post') >= config.min_match
         ).withColumn('post', explode('post')
         ).select(*f1, *f2, col('post.*')
         ).groupBy(*f2, 'post2', 'df'
         ).agg(collect_list(struct('uid', 'post')).alias('alg'),
               collect_set(struct('uid', struct(*[f for f in f1 if f != 'uid']))).alias('meta')
         ).groupBy(*f2
         ).agg(sort_array(collect_list(struct('post2', 'df', 'alg'))).alias('post'),
               map_from_entries(flatten(collect_set('meta'))).alias('meta')
         ).withColumn('src', vit_src('post')
         ).write.mode('ignore').parquet(srcFname)

    srcmap = spark.read.load(srcFname)

    ## Should get uid data from meta
    srcmap.withColumn('src', arrays_zip(col('src'),
                                        expr('transform(src, s -> meta[s.uid])'))
         ).drop('post', 'meta', 'info'
         ).select(*f2, explode('src').alias('src')
         ).select(*f2, col('src.src.*'), col('src.1.*')
         ).join(termCorpus.select('uid', col(config.text).alias('text')), 'uid'
         ).withColumn('text', col('text').substr(col('begin') + 1, col('end') - col('begin'))
         ).join(termCorpus.select(col('uid').alias('uid2'), col(config.text).alias('text2')),
                'uid2'
         ).withColumn('text2',
                      col('text2').substr(col('begin2') + 1, col('end2') - col('begin2'))
         ).write.json(outFname)


    spark.stop()
