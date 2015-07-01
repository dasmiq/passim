import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
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

import java.security.MessageDigest
import java.nio.ByteBuffer

case class imgCoord(val x: Int, val y: Int, val w: Int, val h: Int) {
  def x2 = x + w
  def y2 = y + h
}

case class pageLoc(page: String, loc: imgCoord)

case class TokDoc(name: String, text: String, metadata: Map[String,String], series: String,
		  terms: Array[String],
		  termCharBegin: Array[Int], termCharEnd: Array[Int], termPage: Array[pageLoc])

case class IdSeries(id: Long, series: Long)

case class SpanMatch(uid: Long, begin: Int, end: Int, mid: Long)

case class Span(begin: Int, end: Int, mid: Long)

object CorpusFun {
  def rowToMap(r: Row): Map[String, String] = {
    val vals = r.toSeq.map(_.toString).toArray
    r.schema.fieldNames.zip(vals).toMap
  }
  def parseDocument(m: Map[String,String]): TokDoc = {
    var tokp = new Parameters
    tokp.set("fields", List("pb", "w"))
    val tok = new TagTokenizer(new FakeParameters(tokp))

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
  def parseDocument(r: Row): TokDoc = {
    parseDocument(rowToMap(r))
  }
  def passageURL(doc: TokDoc, begin: Int, end: Int): String = {
    val m = doc.metadata
    var res = new StringBuilder
    if ( m.contains("url") ) {
      res ++= m("url")
      if ( m.contains("pageurl") && doc.termPage.size > end ) {
	val (page, locs) = doc.termPage.slice(begin, end).groupBy(_.page).head
	res ++= m("pageurl").format(page)
	if ( m.contains("imgurl") ) {
	  val x1 = locs.map(_.loc.x).min / 4
	  val y1 = locs.map(_.loc.y).min / 4
	  val x2 = locs.map(_.loc.x2).max / 4
	  val y2 = locs.map(_.loc.y2).max / 4
	  if ( x2 > 0 && y2 > 0 )
	    res ++= m("imgurl").format(600, 600, x1, y1, x2, y2)
	}
      }
    }
    res.toString
  }
  def crossCounts(sizes: Array[Int]): Int = {
    var res: Int = 0
    for ( i <- 0 until sizes.size ) {
      for ( j <- (i + 1) until sizes.size ) {
	res += sizes(i) * sizes(j)
      }
    }
    res
  }
  def indexNgrams(n: Int, maxSeries: Int, corpus: RDD[(IdSeries, TokDoc)]) = {
    val upper = maxSeries * (maxSeries - 1) / 2
    // // We could save space by hasing the n-grams, then checking for
    // // equality when we actually do the alignment, but we need to keep
    // // all the n-gram matches around, not just the ranges.
    // java.nio.ByteBuffer.wrap(java.security.MessageDigest.getInstance("MD5")
    //   .digest("a~b~c~d~e".getBytes).take(8)).getLong
    // val ebuf = new ArrayBuffer[(IdSeries,Int)]()
    corpus.flatMap(d => {
      val (id, doc) = d
      doc.terms.zipWithIndex.sliding(n)
	.map(x => (x.map(_._1).mkString("~"), (id, x(0)._2)))
    })
    .groupByKey
  }
}

object PassFun {
  def increasingMatches(matches: Iterable[(Int,Int,Int)]): Array[(Int,Int,Int)] = {
    val in = matches.toArray.sorted
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

  def gappedMatches(n: Int, gapSize: Int, matches: Array[(Int, Int, Int)]) = {
    val N = matches.size
    var i = 0
    var res = new ListBuffer[((Int,Int), (Int,Int))]
    for ( j <- 0 until N ) {
      val j1 = j + 1
      if ( j == (N-1) || ((matches(j1)._1 - matches(j)._1) * (matches(j1)._2 - matches(j)._2)) > gapSize) {
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
  }

  def edgeText(extent: Int, n: Int, id: IdSeries, terms: Array[String], span: (Int,Int)) = {
    val (start, end) = span
    (id, span,
     if (start <= 0) "" else terms.slice(Math.max(0, start - extent), start + n).mkString(" "),
     if (end >= terms.size) "" else terms.slice(end + 1 - n, Math.min(terms.size, end + 1 + extent)).mkString(" "))
  }

  type Passage = (IdSeries, (Int, Int), String, String)
  def alignEdges(matchMatrix: jaligner.matrix.Matrix, n: Int, minAlg: Int,
		 pid: Long, pass1: Passage, pass2: Passage) = {
    val (id1, span1, prefix1, suffix1) = pass1
    val (id2, span2, prefix2, suffix2) = pass2
    var (s1, e1) = span1
    var (s2, e2) = span2

    if ( s1 > 0 && s2 > 0 ) {
      val palg = jaligner.SmithWatermanGotoh.align(new jaligner.Sequence(prefix1),
						   new jaligner.Sequence(prefix2),
						   matchMatrix, 5, 0.5f)
      val ps1 = palg.getSequence1()
      val ps2 = palg.getSequence2()
      val plen1 = ps1.size - ps1.count(_ == '-')
      val plen2 = ps2.size - ps2.count(_ == '-')
	
      if ( ps1.size > 0 && ps2.size > 0 && palg.getStart1() + plen1 >= prefix1.size
	   && palg.getStart2() + plen2 >= prefix2.size ) {
	val pextra = palg.getIdentity() - prefix1.split(" ").takeRight(n).mkString(" ").size
	if ( pextra > 2 ) {
	  s1 -= ps1.count(_ == ' ') - (if (ps1(0) == ' ') 1 else 0) - n + 1
	  s2 -= ps2.count(_ == ' ') - (if (ps2(0) == ' ') 1 else 0) - n + 1
	  // println((id1,id2))
	  // println("prefix extra: " + pextra)
	  // println(ps1.mkString)
	  // println(palg.getMarkupLine().mkString)
	  // println(ps2.mkString)
	}
      }
    }

    if ( suffix1.size > 0 && suffix2.size > 0 ) {
      val salg = jaligner.SmithWatermanGotoh.align(new jaligner.Sequence(suffix1),
						   new jaligner.Sequence(suffix2),
						   matchMatrix, 5, 0.5f)
      val ss1 = salg.getSequence1()
      val ss2 = salg.getSequence2()
	
      if ( ss1.size > 0 && ss2.size > 0 && salg.getStart1() == 0 && salg.getStart2() == 0 ) {
	val sextra = salg.getIdentity() - suffix1.split(" ").take(n).mkString(" ").size
	if ( sextra > 2 ) {
	  e1 += ss1.count(_ == ' ') - (if (ss1(ss1.size - 1) == ' ') 1 else 0) - n + 1
	  e2 += ss2.count(_ == ' ') - (if (ss2(ss2.size - 1) == ' ') 1 else 0) - n + 1
	  // println((id1,id2))
	  // println("suffix extra: " + sextra)
	  // println(ss1.mkString)
	  // println(salg.getMarkupLine().mkString)
	  // println(ss2.mkString)
	}
      }
    }

    if ( ( e1 - s1 ) >= minAlg && ( e2 - s2 ) >= minAlg )
      List((id1, ((s1, e1), pid)),
	   (id2, ((s2, e2), pid)))
    else
      Nil
  }
  
  def linkSpans(rover: Double,
		init: List[((Int, Int), Array[Long])]): Array[((Int, Int), Array[Long])] = {
    var passages = new ArrayBuffer[((Int, Int), Array[Long])]
    // We had sorted in descreasing order of span length, but that leaves gaps in the output.
    for ( cur <- init ) { //.sortWith((a, b) => (a._1._1 - a._1._2) < (b._1._1 - b._1._2)) ) {
      val curLen = cur._1._2 - cur._1._1
      val N = passages.size
      var pmod = false
      for ( i <- 0 until N; if !pmod ) {
	val pass = passages(i)
	if ( Math.max(0.0, Math.min(cur._1._2, pass._1._2) - Math.max(cur._1._1, pass._1._1)) / Math.max(curLen, pass._1._2 - pass._1._1) > rover ) {
	  passages(i) = ((Math.min(cur._1._1, pass._1._1),
			  Math.max(cur._1._2, pass._1._2)),
			 pass._2 ++ cur._2)
	  pmod = true
	}
      }
      if (!pmod) {
	passages += cur
      }
    }
    passages.toArray
  }
  
  def mergeSpans(rover: Double, init: Iterable[((Int, Int), Long)]): Seq[((Int, Int), Array[Long])] = {
    val in = init.toArray.sorted
    var top = -1
    var passages = new ListBuffer[((Int, Int), Array[Long])]
    var spans = new ListBuffer[((Int, Int), Array[Long])]
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
      spans += ((span, Array(cur._2)))
    }
    passages ++= linkSpans(rover, spans.toList)
    passages.toList
  }
}

object BoilerApp {
  def hapaxIndex(n: Int, w: Seq[String]) = {
    w.sliding(n)
      .map(_.mkString("~"))
      .zipWithIndex
      .toArray
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .filter(_._2.size == 1)
      .mapValues(_(0))
  }
  
  def main(args: Array[String]) = {
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

    val corpus = data.sort("series", "date")
      .map(CorpusFun.parseDocument)
      .zipWithUniqueId
      .map(x => (IdSeries(x._2, 0), x._1))

    val n = 3
    val minAlg = 20
    val relOver = 0.8
    val gap = 50

    val gap2 = gap * gap

    val matchMatrix = jaligner.matrix.MatrixGenerator.generate(2, -1)

    val pass = corpus
      .mapValues(d => (d, hapaxIndex(n, d.terms)))
      .sliding(3)
      .flatMap(x => {
	val (cid, (cdoc, cidx)) = x.last
	val m = cidx.toMap
	x.filter(_._1 != cid)
	  .flatMap(y => {
	    val (pid, (pdoc, pidx)) = y
	    val inc = PassFun.increasingMatches(pidx
	      .flatMap(z => if (m.contains(z._1)) Some((z._2, m(z._1), 1)) else None))
	    PassFun.gappedMatches(n, gap2, inc)
	    .map(z => PassFun.alignEdges(matchMatrix, n, minAlg, 0,
					 PassFun.edgeText(gap, n, pid, pdoc.terms, z._1),
	      				 PassFun.edgeText(gap, n, cid, cdoc.terms, z._2))
	       )
	    .filter(_.size > 0)
	    .map(z => {
	      val (pbegin, pend) = z.head._2._1
	      val (begin, end) = z.last._2._1
	      Map("id" -> cdoc.name,
		  "uid" -> cid.id,
		  "title" -> cdoc.metadata.getOrElse("title", ""),
    		  "date" -> cdoc.metadata.getOrElse("date", ""),
    		  "url" -> CorpusFun.passageURL(cdoc, begin, end),
    		  "start" -> begin,
    		  "end" -> end,
    		  "text" -> cdoc.text.substring(cdoc.termCharBegin(begin),
    						cdoc.termCharEnd(end)),
		  "pid" -> pdoc.name,
		  "puid" -> pid.id,
		  "pdate" -> pdoc.metadata.getOrElse("date", ""),
		  "purl" -> CorpusFun.passageURL(pdoc, pbegin, pend),
		  "pstart" -> pbegin,
		  "pend" -> pend,
		  "ptext" -> pdoc.text.substring(pdoc.termCharBegin(pbegin),
    						 pdoc.termCharEnd(pend)))
	    })
	  })
      })

    pass
      .map(x => {
	val mapper  = new ObjectMapper()
	mapper.registerModule(DefaultScalaModule)
	mapper.writeValueAsString(x)
      })
    .saveAsTextFile(args(1) + ".align")

    pass
      .map(x => (x("id").toString, ((x("start").asInstanceOf[Int], x("end").asInstanceOf[Int]), 0L)))
      .groupByKey
      .mapValues(s => {
	val res = PassFun.mergeSpans(0, s).map(_._1).toArray
	res.sorted
      })
      .map(x => {
	val mapper  = new ObjectMapper()
	mapper.registerModule(DefaultScalaModule)
	mapper.writeValueAsString(Map("id" -> x._1,
				      "spans" -> x._2))
      })
      .saveAsTextFile(args(1))
  }
}

case class TokText(terms: Array[String], termCharBegin: Array[Int], termCharEnd: Array[Int],
  pages: Array[String], imgLocs: Array[imgCoord])

object ImportApp {
  def tokenize(text: String): TokText = {
    var tokp = new Parameters
    tokp.set("fields", List("pb", "w"))
    val tok = new TagTokenizer(new FakeParameters(tokp))

    var d = new Document("raw", text)
    tok.tokenize(d)

    var pages = new ArrayBuffer[String]
    var locs = new ArrayBuffer[imgCoord]
    var curPage = ""
    var curCoord = imgCoord(0, 0, 0, 0)
    var idx = 0
    val p = """^(\d+),(\d+),(\d+),(\d+)$""".r
    for ( t <- d.tags ) {
      val off = t.begin
      while ( idx < off ) {
        pages += curPage
	locs += curCoord
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
        pages += curPage
	locs += curCoord
	idx += 1
      }
    }

    TokText(d.terms.toSeq.toArray,
      d.termCharBegin.map(_.toInt).toArray,
      d.termCharEnd.map(_.toInt).toArray,
      pages.toArray, locs.toArray)
  }
  def hashString(s: String): Long = {
    ByteBuffer.wrap(
      MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8"))
    ).getLong
  }
  def preprocessText(raw: DataFrame): DataFrame = {
    val hashId = udf {(id: String) => hashString(id)}
    val tok = udf {(text: String) => tokenize(text)}
    val tokenized = raw.filter(!raw("id").isNull && !raw("text").isNull)
      .withColumn("uid", hashId(raw("id")))
      .withColumn("tok", tok(raw("text")))
    val col = tokenized("tok")
    tokenized
      .withColumn("terms", col("terms"))
      .withColumn("termCharBegin", col("termCharBegin"))
      .withColumn("termCharEnd", col("termCharEnd"))
      .withColumn("pages", col("pages"))
      .withColumn("imgLocs", col("imgLocs"))
      .drop("tok")
  }
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Import Application")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[imgCoord], classOf[IdSeries], classOf[TokText]))
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val raw = sqlContext.read.json(args(0))

    val proc = preprocessText(raw)

    // Do some checks here?

    proc.write.parquet(args(1))
  }
}

object PassimApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Passim Application")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[imgCoord],
				 classOf[SpanMatch], classOf[IdSeries]))
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val inFile = args(0)

    val raw = if ( inFile.endsWith(".parquet") ) {
      sqlContext.read.parquet(inFile)
    } else {
      ImportApp.preprocessText(sqlContext.read.json(inFile))
    }

    // Do simple series transformation; in future, could join with metadata.
    val sname = udf {(x: String) => x.split("[_/]")(0) }
    val corpus = {
      if ( raw.columns.contains("series") )
        raw
      else
        raw.withColumn("series", sname(raw("id")))
    }

    val maxSeries: Int = 100
    val n: Int = 5
    val minRep: Int = 5
    val minAlg: Int = 20
    val gap: Int = 100
    val relOver = 0.5
    val maxRep: Int = 4

    val gap2 = gap * gap
    val upper = maxSeries * (maxSeries - 1) / 2

    val pairs = corpus
      .select("uid", "series", "terms") // cache this projection?
      .flatMap({
        case Row(uid: Long, series: String, terms: Seq[String]) => {
          terms.zipWithIndex.sliding(n)
            .map(x => (x.map(_._1).mkString("~"),
              (IdSeries(uid, ImportApp.hashString(series)), x(0)._2)))
        }
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
	&& ( CorpusFun.crossCounts(x._2.map(p => p._1.series)
	  .groupBy(identity).map(_._2.size).toArray) ) <= upper)
      .mapValues(x => x.groupBy(_._1).toArray.map(p => (p._1, p._2.map(_._2).toArray)).toArray)
      .flatMap(x => for ( a <- x._2; b <- x._2; if a._1.id < b._1.id && a._1.series != b._1.series && a._2.size == 1 && b._2.size == 1 ) yield ((a._1.id, b._1.id), (a._2(0), b._2(0), x._2.size)))
      .groupByKey.filter(_._2.size >= minRep)
      .mapValues(PassFun.increasingMatches)
      .filter(_._2.size >= minRep)
      .flatMapValues(PassFun.gappedMatches(n, gap2, _))

    // pairs.saveAsTextFile(args(1) + ".pairs")

    // Unique IDs will serve as edge IDs in connected component graph
    val pass1 = pairs.zipWithUniqueId
      .flatMap(x => {
        val (((uid1, uid2), ((s1, e1), (s2, e2))), mid) = x
        Array(SpanMatch(uid1, s1, e1, mid),
          SpanMatch(uid2, s2, e2, mid))
      })

    val matchMatrix = jaligner.matrix.MatrixGenerator.generate(2, -1)
    val pass2 = pass1.toDF
      .join(corpus.select("uid", "series", "terms"), "uid")
      .map({
        case Row(uid: Long, begin: Int, end: Int, mid: Long,
          series: String, terms: Seq[String]) => {
          (mid, PassFun.edgeText(gap * 2/3, n, IdSeries(uid, ImportApp.hashString(series)),
            terms.toArray, (begin, end)))
        }
      })
      .groupByKey
      .flatMap(x => {
        val pass = x._2.toArray
        PassFun.alignEdges(matchMatrix, n, minAlg, x._1, pass(0), pass(1))
      })

    val pass = pass2
      .groupByKey
      .flatMapValues(PassFun.mergeSpans(relOver, _))
      .zipWithUniqueId
    // pass.saveAsTextFile(args(1) + ".pass")

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
      (id, pass, cid) => (pass._1, (pass._2, cid.toLong))
    }
      .values
      .groupBy(_._2._2)
      .filter(x => {
        x._2.groupBy(_._1.id).values.groupBy(_.head._1.series).map(_._2.size).max <= maxRep
      })
      .flatMap(_._2)
      .map(x => (x._1.id, x._2))

    // clusters.saveAsTextFile(args(1) + ".clusters")

    val clusterInfo = clusters.groupByKey
      .mapValues(v => v.map(x => Span(x._1._1, x._1._2, x._2)).toArray)
      .toDF("uid", "passages")
      .join(corpus, "uid")
      .flatMap(x => {
        val uid = x.getLong(0)
        val passages = x.getSeq[Row](1)
        val m = CorpusFun.rowToMap(x)

        passages.map({
          case Row(begin: Int, end: Int, cid: Long) => ((begin, end), cid)
        })
          .groupBy(_._2).values.flatMap(p => {
            PassFun.mergeSpans(0, p).map(z => (z._1, z._2(0)))
          }).map(p => {
            val ((begin, end), cid) = p
            (cid,
              Map("id" -> m("id"),
                "uid" -> uid,
                "name" -> m.getOrElse("series", ""),
                "title" -> m.getOrElse("title", ""),
                "date" -> m.getOrElse("date", ""),
                // "url" -> CorpusFun.passageURL(doc, begin, end),
                "start" -> begin,
                "end" -> end,
                "text" -> m("text").substring(x.getSeq[Int](x.fieldIndex("termCharBegin"))(begin),
                  x.getSeq[Int](x.fieldIndex("termCharEnd"))(end))))
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
