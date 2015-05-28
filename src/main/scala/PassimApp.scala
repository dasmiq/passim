import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

import org.lemurproject.galago.tupleflow.Parameters
import org.lemurproject.galago.tupleflow.FakeParameters
import org.lemurproject.galago.core.parse.Document
import org.lemurproject.galago.core.parse.TagTokenizer

import collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

case class imgCoord(val x: Int, val y: Int, val w: Int, val h: Int) {
  def x2 = x + w
  def y2 = y + h
}

case class pageLoc(page: String, loc: imgCoord)

case class TokDoc(name: String, text: String, metadata: Map[String,String], series: String,
		  terms: Array[String],
		  termCharBegin: Array[Int], termCharEnd: Array[Int], termPage: Array[pageLoc])

case class IdSeries(id: Long, series: Long)

object CorpusFun {
  def parseDocument(r: Row): TokDoc = {
    var tokp = new Parameters
    tokp.set("fields", List("pb", "w"))
    val tok = new TagTokenizer(new FakeParameters(tokp))

    val names = r.schema.fieldNames
    val m = (for ( i <- 0 until names.size ) yield (names(i), r.getString(i))).toMap

    var d = new Document(m("id"), m("text"))
    tok.tokenize(d)

    var loc = new ArrayBuffer[pageLoc]
    var curPage = ""
    var curCoord = imgCoord(0, 0, 0, 0)
    var idx = 0
    val p = """^(\d+),(\d+),(\d+),(\d+)$""".r
    for ( t <- d.tags ) {
      val off = t.begin
      while ( idx < off ) {
	loc += pageLoc(curPage, curCoord)
	idx += 1
      }
      if ( t.name == "pb" )
	curPage = t.attributes.getOrElse("n", "")
      else if ( t.name == "w" ) {
    	  t.attributes.getOrElse("coords", "") match {
            case p(x, y, w, h) => curCoord = imgCoord(x.toInt, y.toInt, w.toInt, h.toInt)
    	    case _ => curCoord
    	  }
    	}
    }
    if ( idx > 0 ) {
      while ( idx < d.terms.size ) {
	loc += pageLoc(curPage, curCoord)
	idx += 1
      }
    }

    TokDoc(m("id"), m("text"),
	   m - "id" - "text" - "series",
	   m("series"),
	   d.terms.toSeq.toArray,
	   d.termCharBegin.map(_.toInt).toArray,
	   d.termCharEnd.map(_.toInt).toArray,
	   loc.toArray)
  }
}
  
class NgramIndexer(val n: Int, val maxSeries: Int) extends Serializable {
  def index(corpus: RDD[(IdSeries, TokDoc)]) = {
    val n_ = n
    val upper = maxSeries * (maxSeries - 1) / 2
    def crossCounts(sizes: Array[Int]): Int = {
      var res: Int = 0
      for ( i <- 0 until sizes.size ) {
	for ( j <- (i + 1) until sizes.size ) {
	  res += sizes(i) * sizes(j)
	}
      }
      res
    }
    // // We could save space by hasing the n-grams, then checking for
    // // equality when we actually do the alignment, but we need to keep
    // // all the n-gram matches around, not just the ranges.
    // java.nio.ByteBuffer.wrap(java.security.MessageDigest.getInstance("MD5")
    //   .digest("a~b~c~d~e".getBytes).take(8)).getLong
    // val ebuf = new ArrayBuffer[(IdSeries,Int)]()
    corpus.flatMap(d => {
      val (id, doc) = d
      doc.terms.zipWithIndex.sliding(n_)
	.map(x => (x.map(_._1).mkString("~"), (id, x(0)._2)))
    })
    .groupByKey
    // // Save this implementation in case we see lots of high-frequency n-grams
    // .aggregateByKey(ebuf)(
    //   (buf, v) => if ( buf.size <= upper ) buf += v else buf,
    //   (c1, c2) => {
    // 	if ( c1.size > upper )
    // 	  c1
    // 	else {
    // 	  if (c2.size > upper )
    // 	    c2
    // 	  else
    // 	    c1 ++= c2
    // 	}
    //   })
    .filter(x => x._2.size >= 2 && x._2.size <= upper
	    && ( crossCounts(x._2.map(p => p._1.series)
			     .groupBy(identity).map(_._2.size).toArray) ) <= upper)
    .mapValues(x => x.groupBy(_._1).toArray.map(p => (p._1, p._2.map(_._2).toArray)).toArray)
  }
}

object PairFun {
  def increasingMatches(matches: Array[(Int,Int,Int)]): Array[(Int,Int,Int)] = {
    val in = matches.sorted
    val X = in.map(_._2).toArray
    val N = X.size
    var P = Array.fill(N)(0)
    var M = Array.fill(N + 1)(0)
    var L = 0
    for ( i <- 0 until N ) {
      var low = 1
      var high = L
      while ( low <= high ) {
	val mid = Math.ceil( (low + high) / 2).toInt
	if ( X(M(mid)) < X(i) )
	  low = mid + 1
	else
	  high = mid - 1
      }
      val newL = low
      P(i) = M(newL - 1)
      M(newL) = i
      if ( newL > L ) L = newL
    }
    // Backtrace
    var res = Array.fill(L)((0,0,0))
    var k = M(L)
    for ( i <- (L - 1) to 0 by -1 ) {
      res(i) = in(k)
      k = P(k)
    }
    res.toArray
  }
}

object PassimApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Passim Application")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[imgCoord], classOf[pageLoc],
				 classOf[TokDoc], classOf[IdSeries]))
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val raw = sqlContext.jsonFile(args(0))
    // Do simple series transformation; in future, could join with metadata.
    val sname = udf {(x: String) => x.split("[_/]")(0) }
    val data = raw.withColumn("series", sname(raw("id")))

    val rawCorpus = data.map(CorpusFun.parseDocument)
      .zipWithUniqueId
      .map(x => (x._2, x._1))
    rawCorpus.persist(StorageLevel.MEMORY_AND_DISK_SER)

    rawCorpus.mapValues(x => (x.name, x.terms.size)).saveAsTextFile(args(1) + ".names")

    val series = rawCorpus.map(x => (x._2.series, x._1))
      .reduceByKey((a, b) => Math.min(a, b))
      .toLocalIterator.toMap

    val corpus = rawCorpus.map(x => series(x._2.series)).zip(rawCorpus)
      .map(x => (IdSeries(x._2._1, x._1), x._2._2))
      //.partitionBy(new org.apache.spark.HashPartitioner(50))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    rawCorpus.unpersist()

    val maxSeries: Int = 100
    val n: Int = 5
    val minRep: Int = 5
    val minAlg: Int = 20
    val gap: Int = 100
    val relOver = 0.5
    val maxRep: Int = 4

    val gap2 = gap * gap

    val indexer = new NgramIndexer(n, maxSeries)

    val pairs = indexer.index(corpus)
      .flatMap(x => for ( a <- x._2; b <- x._2; if a._1.id < b._1.id && a._1.series != b._1.series && a._2.size == 1 && b._2.size == 1 ) yield ((a._1, b._1), (a._2(0), b._2(0), x._2.size)))
      .groupByKey.filter(x => x._2.size >= minRep)
      .mapValues(x => PairFun.increasingMatches(x.toArray))
      .filter(x => x._2.size >= minRep)
      .flatMapValues(matches => {
	val N = matches.size
	var i = 0
	var res = new ListBuffer[((Int,Int), (Int,Int))]
	for ( j <- 0 until N ) {
	  val j1 = j + 1
	  if ( j == (N-1) || ((matches(j1)._1 - matches(j)._1) * (matches(j1)._2 - matches(j)._2)) > gap2) {
	    // This is where we'd score the spans
	    if ( j > i && (matches(j)._1 - matches(i)._1 + n - 1) >= 10
		 && (matches(j)._2 - matches(i)._2 + n - 1) >= 10) {
	      res += (((matches(i)._1, matches(j)._1 + n - 1),
		       (matches(i)._2, matches(j)._2 + n - 1)))
	    }
	    i = j1
	  }
	}
	res.toList
      })

    // pairs.saveAsTextFile(args(1) + ".pairs")

    def linkSpans(rover: Double,
		  init: List[((Int, Int), Long)]): Array[((Int, Int), Array[Long])] = {
      var passages = new ArrayBuffer[((Int, Int), Array[Long])]
      for ( cur <- init.sortWith((a, b) => (a._1._1 - a._1._2) < (b._1._1 - b._1._2)) ) {
	val curLen = cur._1._2 - cur._1._1
	val N = passages.size
	var pmod = false
	for ( i <- 0 until N; if !pmod ) {
	  val pass = passages(i)
	  if ( Math.max(0.0, Math.min(cur._1._2, pass._1._2) - Math.max(cur._1._1, pass._1._1)) / Math.max(curLen, pass._1._2 - pass._1._1) > rover ) {
	    passages(i) = ((Math.min(cur._1._1, pass._1._1),
			    Math.max(cur._1._2, pass._1._2)),
			   pass._2 ++ Array(cur._2))
	    pmod = true
	  }
	}
	if (!pmod) {
	  passages += ((cur._1, Array(cur._2)))
	}
      }
      passages.toArray
    }

    def mergeSpans(rover: Double, init: Array[((Int, Int), Long)]): Seq[((Int, Int), Array[Long])] = {
      val in = init.sorted
      var top = -1
      var passages = new ListBuffer[((Int, Int), Array[Long])]
      var spans = new ListBuffer[((Int, Int), Long)]
      for ( cur <- in ) {
	val span = cur._1
	if ( span._1 > top ) {
	  top = span._2
	  passages ++= linkSpans(rover, spans.toList)
	  spans.clear
	}
	else {
	  top = Math.max(top, span._2)
	}
	spans += cur
      }
      passages ++= linkSpans(rover, spans.toList)
      passages.toList
    }

    // Unique IDs will serve as edge IDs in connected component graph
    val pass = pairs.zipWithUniqueId
      .flatMap(x => Array((x._1._1._1, (x._1._2._1, x._2)),
			  (x._1._1._2, (x._1._2._2, x._2)))
	     )
    .groupByKey				// This is where we'd extend the spans by alignment.
    .flatMapValues(x => mergeSpans(relOver, x.toArray))
    .zipWithUniqueId

    val passNodes = pass.map(v => {
      val ((doc, (span, edges)), id) = v
      (id, (doc, span))
    })
    val passEdges = pass.flatMap(v => {
      val ((doc, (span, edges)), id) = v
      edges.map(e => (e, id))
    }).groupByKey
    .map(e => {
      val nodes = e._2.toArray.sorted
      Edge(nodes(0), nodes(1), 1)
    })

    val passGraph = Graph(passNodes, passEdges)
    passGraph.cache()

    val cc = passGraph.connectedComponents()

    val clusters = passGraph.vertices.innerJoin(cc.vertices){
      (id, pass, cid) => (pass._1, (pass._2, cid))
    }
    .values
    .groupBy(_._2._2)
    .filter(x => {
      x._2.groupBy(_._1.id).values.groupBy(_.head._1.series).map(_._2.size).max <= maxRep
    })
    .flatMap(_._2)

    // clusters.saveAsTextFile(args(1) + ".clusters")

    val clusterInfo = clusters.groupByKey
      .join(corpus)
      .flatMap(x => {
	val (id, (passages, doc)) = x
	passages.groupBy(_._2).values.flatMap(p => {
	  mergeSpans(0, p.toArray).map(z => (z._1, z._2(0)))
	}).map(p => {
	  val ((begin, end), cid) = p
	  (cid,
	   Map("id" -> doc.name,
	       "uid" -> id.id,
	       "name" -> doc.series,
	       "title" -> doc.metadata.getOrElse("title", null),
	       "date" -> doc.metadata.getOrElse("date", ""),
	       "url" -> doc.metadata.getOrElse("url", null), // should get page coords
	       "start" -> begin,
	       "end" -> end,
	       "text" -> doc.text.substring(doc.termCharBegin(begin),
					    doc.termCharEnd(end))))
	})
      })
    .groupByKey
    .sortBy(_._2.size, ascending=false)
    .map(x => {
      val (cid, members) = x
      val mapper  = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.writeValueAsString(
	Map("id" -> cid,
	    "size" -> members.size,
	    "members" -> members.toArray.sortWith((a, b) => a("date").toString < b("date").toString)))
    })
    clusterInfo.saveAsTextFile(args(1))
  }
}
