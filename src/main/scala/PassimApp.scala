package passim

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import org.apache.hadoop.fs.{FileSystem,Path}

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import java.sql.Date

import collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import java.security.MessageDigest
import java.nio.ByteBuffer

case class Config(version: String = BuildInfo.version,
  mode: String = "cluster",
  n: Int = 5, maxDF: Int = 100, minRep: Int = 5, minAlg: Int = 20,
  gap: Int = 100, relOver: Double = 0.8, maxRep: Int = 10, history: Int = 7,
  wordLength: Double = 2,
  pairwise: Boolean = false, docwise: Boolean = false,
  id: String = "id", group: String = "series", text: String = "text",
  inputFormat: String = "json", outputFormat: String = "json",
  inputPaths: String = "", outputPath: String = "") {
  def save(fname: String, sqlContext: SQLContext) {
    import sqlContext.implicits._
    sqlContext.sparkContext.parallelize(this :: Nil).toDF.coalesce(1).write.json(fname)
  }
}

case class imgCoord(val x: Int, val y: Int, val w: Int, val h: Int) {
  def x2 = x + w
  def y2 = y + h
}

// Could parameterized on index type instead of Int
case class Span(val begin: Int, val end: Int) {
  def length = end - begin
  def size = this.length
  def union(that: Span): Span = {
    Span(Math.min(this.begin, that.begin), Math.max(this.end, that.end))
  }
  def intersect(that: Span): Span = {
    val res = Span(Math.max(this.begin, that.begin), Math.min(this.end, that.end))
    if ( res.begin >= res.end ) Span(0, 0) else res
  }
}

case class Post(uid: Long, gid: Long, tf: Int, post: Int)

case class IdSeries(id: Long, series: Long)

case class PassAlign(id1: String, id2: String,
  s1: String, s2: String, b1: Int, e1: Int, n1: Int, b2: Int, e2: Int, n2: Int,
  matches: Int, score: Float)

case class BoilerPass(id: String, termCount: Int,
  passageBegin: Array[Int], passageEnd: Array[Int], passageLastId: Array[String],
  alignments: Array[PassAlign])

case class NewDoc(newid: String, newtext: String, aligned: Boolean)

case class ClusterParent(id: String, begin: Long, date: String, matchProp: Float, score: Float)

object CorpusFun {
  def boundingBox(regions: Array[imgCoord]): imgCoord = {
    // The right thing to do here is to give imgCoord a merge
    // operation usable with reduce so that we can make only one pass
    // through regions.
    val x1 = regions.map(_.x).min
    val y1 = regions.map(_.y).min
    val x2 = regions.map(_.x2).max
    val y2 = regions.map(_.y2).max
    imgCoord(x1, y1, x2 - x1, y2 - y1)
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

  def gappedMatches(n: Int, gapSize: Int, minAlg: Int, matches: Array[(Int, Int, Int)]) = {
    val N = matches.size
    var i = 0
    var res = new ListBuffer[((Int,Int), (Int,Int))]
    for ( j <- 0 until N ) {
      val j1 = j + 1
      if ( j == (N-1) || (matches(j1)._1 - matches(j)._1) > gapSize || (matches(j1)._2 - matches(j)._2) > gapSize) {
	// This is where we'd score the spans
	if ( j > i && (matches(j)._1 - matches(i)._1 + n - 1) >= minAlg
	     && (matches(j)._2 - matches(i)._2 + n - 1) >= minAlg) {
	  res += (((matches(i)._1, matches(j)._1 + n - 1),
		   (matches(i)._2, matches(j)._2 + n - 1)))
	}
	i = j1
      }
    }
    res.toList
  }

  def edgeText(extent: Int, n: Int, id: IdSeries, terms: Array[String], span: Span) = {
    val (start, end) = (span.begin, span.end)
    (id, span,
     if (start <= 0) "" else terms.slice(Math.max(0, start - extent), start + n).mkString(" "),
     if (end >= terms.size) "" else terms.slice(end + 1 - n, Math.min(terms.size, end + 1 + extent)).mkString(" "))
  }

  type Passage = (IdSeries, Span, String, String)
  def alignEdges(matchMatrix: jaligner.matrix.Matrix, n: Int, minAlg: Int,
		 pid: Long, pass1: Passage, pass2: Passage) = {
    val (id1, span1, prefix1, suffix1) = pass1
    val (id2, span2, prefix2, suffix2) = pass2
    var (s1, e1) = (span1.begin, span1.end)
    var (s2, e2) = (span2.begin, span2.end)

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
	}
      }
    }

    if ( ( e1 - s1 ) >= minAlg && ( e2 - s2 ) >= minAlg )
      List((id1, (Span(s1, e1), pid)),
	   (id2, (Span(s2, e2), pid)))
    else
      Nil
  }

  // TODO: Could be sped up with an interval tree (though, e.g.,
  // Guava's RangeTree is unsuitable since ranges can't overlap).
  def mergeSpans(rover: Double, init: Iterable[(Span, Long)]): Seq[(Span, ArrayBuffer[Long])] = {
    val res = ArrayBuffer[(Span, ArrayBuffer[Long])]()
    val in = init.toArray.sortWith((a, b) => a._1.length < b._1.length)
    for ( cur <- in ) {
      val span = cur._1
      var idx = -1
      var best = 0.0
      for ( i <- 0 until res.size ) {
        val s = res(i)._1
        val score = 1.0 * span.intersect(s).length / span.union(s).length
        if ( score > rover && score > best ) {
          idx = i
          best = score
        }
      }
      if ( idx < 0 ) {
        res += ((span, ArrayBuffer(cur._2)))
      } else {
        val rec = ((res(idx)._1.union(span), res(idx)._2 ++ ArrayBuffer(cur._2)))
        res(idx) = rec
      }
    }
    res.toSeq
  }

  type DocPassage = (String, Int, Int, Int, String)
  case class AlignedPassage(s1: String, s2: String, b1: Int, b2: Int, matches: Int, score: Float)
  def alignStrings(n: Int, gap: Int, matchMatrix: jaligner.matrix.Matrix,
    d1: DocPassage, d2: DocPassage): PassAlign = {
    val (id1, b1, e1, n1, s1) = d1
    val (id2, b2, e2, n2, s2) = d2
    val chunks = recursivelyAlignStrings(n, gap * gap, matchMatrix,
      s1.replaceAll("-", "_"), s2.replaceAll("-", "_"))
    // Could make only one pass through chunks if we implemented a merger for AlignedPassages.
    PassAlign(id1, id2, chunks.map(_.s1).mkString, chunks.map(_.s2).mkString,
      b1, e1, n1, b2, e2, n2,
      chunks.map(_.matches).sum,
      chunks.map(_.score).sum)
  }
  def recursivelyAlignStrings(n: Int, gap2: Int, matchMatrix: jaligner.matrix.Matrix,
    s1: String, s2: String): Seq[AlignedPassage] = {
    val m1 = BoilerApp.hapaxIndex(n, s1)
    val m2 = BoilerApp.hapaxIndex(n, s2)
    val inc = PassFun.increasingMatches(m1
      .flatMap(z => if (m2.contains(z._1)) Some((z._2, m2(z._1), 1)) else None))
    val prod = s1.size * s2.size
    if ( inc.size == 0 && (prod >= gap2 || prod < 0) ) {
      Seq(AlignedPassage(s1 + ("-" * s2.size), ("-" * s1.size) + s2,
        0, 0, 0, -5.0f - 0.5f * s1.size - 0.5f * s2.size))
    } else {
      (Array((0, 0, 0)) ++ inc ++ Array((s1.size, s2.size, 0)))
        .sliding(2).flatMap(z => {
          val (b1, b2, c) = z(0)
          val (e1, e2, _) = z(1)
          val n1 = e1 - b1
          val n2 = e2 - b2
          val chartSize = n1 * n2
          if ( c == 0 && e1 == 0 && e2 == 0 ) {
            Seq()
          } else if ( chartSize <= gap2 && chartSize >= 0 ) { // overflow!
            val p1 = s1.substring(b1, e1)
            val p2 = s2.substring(b2, e2)
            if ( n1 == n2 && p1 == p2 ) {
              Seq(AlignedPassage(p1, p2, b1, b2, p1.size, 2.0f * p2.size))
            } else {
              val alg = jaligner.NeedlemanWunschGotoh.align(new jaligner.Sequence(p1),
                new jaligner.Sequence(p2), matchMatrix, 5, 0.5f)
              // // HACK!! WHY does JAligner swap sequences ?!?!?!?
              val a1 = new String(alg.getSequence2)
              val a2 = new String(alg.getSequence1)
              if ( a1.replaceAll("-", "") == p2 && a2.replaceAll("-", "") == p1 ) {
                Seq(AlignedPassage(a2, a1, b1, b2, alg.getIdentity, alg.getScore))
              } else {
                Seq(AlignedPassage(a1, a2, b1, b2, alg.getIdentity, alg.getScore))
              }
            }
          } else {
            if ( c > 0 ) {
              val len = Math.min(n, Math.min(n1, n2))
              val p1 = s1.substring(b1, b1 + len)
              val p2 = s2.substring(b2, b2 + len)
              Array(AlignedPassage(p1, p2, b1, b2, len, 2.0f * len)) ++
              recursivelyAlignStrings(n, gap2, matchMatrix, s1.substring(b1 + len, e1), s2.substring(b2 + len, e2))
            } else {
              recursivelyAlignStrings(n, gap2, matchMatrix, s1.substring(b1, e1), s2.substring(b2, e2))
            }
          }
        }).toSeq
    }
  }

  def alignTerms(n: Int, gap: Int, matchMatrix: jaligner.matrix.Matrix,
    t1: Array[String], t2: Array[String]): AlignedPassage = {
    val chunks = recursivelyAlignTerms(n, gap * gap, matchMatrix, t1, t2)
    // Could make only one pass through chunks if we implemented a merger for AlignedPassages.
    AlignedPassage(chunks.map(_.s1).mkString(" "), chunks.map(_.s2).mkString(" "),
      0, 0,
      chunks.map(_.matches).sum + chunks.size - 1,
      chunks.map(_.score).sum + (chunks.size - 1) * 2.0f)
  }
  def recursivelyAlignTerms(n: Int, gap2: Int, matchMatrix: jaligner.matrix.Matrix,
    t1: Array[String], t2: Array[String]): Seq[AlignedPassage] = {
    val m1 = BoilerApp.hapaxIndex(n, t1)
    val m2 = BoilerApp.hapaxIndex(n, t2)
    val inc = PassFun.increasingMatches(m1
      .flatMap(z => if (m2.contains(z._1)) Some((z._2, m2(z._1), 1)) else None))
    if ( inc.size == 0 && (t1.size * t2.size) > gap2 ) {
      Seq(AlignedPassage("...", "...", 0, 0, 0, -5.0f - 0.5f * t1.size - 0.5f * t2.size))
    } else {
      (Array((0, 0, 0)) ++ inc ++ Array((t1.size, t2.size, 0)))
        .sliding(2).flatMap(z => {
          val (b1, b2, c) = z(0)
          val (e1, e2, _) = z(1)
          val n1 = e1 - b1
          val n2 = e2 - b2
          if ( c == 0 && e1 == 0 && e2 == 0 ) {
            Seq()
          } else if ( (n1 * n2) <= gap2 ) {
            val s1 = t1.slice(b1, e1).mkString(" ")
            val s2 = t2.slice(b2, e2).mkString(" ")
            if ( n1 == n2 && s1 == s2 ) {
              Seq(AlignedPassage(s1, s2, b1, b2, s1.size, 2.0f * s2.size))
            } else {
              val alg = jaligner.NeedlemanWunschGotoh.align(new jaligner.Sequence(s1),
                new jaligner.Sequence(s2), matchMatrix, 5, 0.5f)
              Seq(AlignedPassage(new String(alg.getSequence1), new String(alg.getSequence2),
                b1, b2, alg.getIdentity, alg.getScore))
            }
          } else {
            if ( c > 0 ) {
              val s1 = t1.slice(b1, b1+n).mkString(" ")
              val s2 = t2.slice(b2, b2+n).mkString(" ")
              // Array(AlignedPassage("TOO", "BIG", b1, b2, 0, 0f)) ++
              Array(AlignedPassage(s1, s2, b1, b2, s1.size, 2.0f * s2.size)) ++
              recursivelyAlignTerms(n, gap2, matchMatrix, t1.slice(b1+n, e1), t2.slice(b2+n, e2))
            } else {
              recursivelyAlignTerms(n, gap2, matchMatrix, t1.slice(b1, e1), t2.slice(b2, e2))
            }
          }
        }).toSeq
    }
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
  def hapaxIndex(n: Int, s: String) = {
    s.sliding(n)
      .zipWithIndex
      .toArray
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .filter(_._2.size == 1)
      .mapValues(_(0))
  }

  def splitDocs(r: Row): Array[NewDoc] = {
    val id = r.getString(0)
    val text = r.getString(1)
    val docs = new ArrayBuffer[NewDoc]
    if ( r.isNullAt(2) ) {
      docs += NewDoc(id, text, false)
    } else {
      val passageBegin = r.getSeq[Int](2).toArray
      val passageEnd = r.getSeq[Int](3).toArray
      val termCharEnd = r.getSeq[Int](4).toArray
      def tcOff(termOff: Int): Int = if ( termOff == 0 ) 0 else termCharEnd(termOff - 1)
      for ( i <- 0 until passageBegin.size ) {
        val begin = passageBegin(i)
        val pBegin = if ( i == 0 ) {
          if ( begin == 0 ) -1 else 0
        } else
          (passageEnd(i - 1) + 1)
        if ( pBegin >= 0 && pBegin < (begin - 1)) {
          docs += NewDoc(id + "_" + pBegin,
            text.substring(tcOff(pBegin), termCharEnd(begin - 1)),
            false)
        }
        docs += NewDoc(id + "_" + begin,
          text.substring(tcOff(begin), termCharEnd(passageEnd(i))),
          true)
      }
      if ( (passageEnd.last + 1) < termCharEnd.size ) {
        val pBegin = passageEnd.last + 1
        docs += NewDoc(id + "_" + pBegin, text.substring(tcOff(pBegin)), false)
      }
    }
    docs.toArray
  }

  // TODO: Unescape other character entities to UTF.
  def cleanXML(s: String): String = {
    s.replaceAll("</?[A-Za-z][^>]*>", "")
      .replaceAll("&quot;", "\"")
      .replaceAll("&apos;", "'")
      .replaceAll("&lt;", "<")
      .replaceAll("&gt;", ">")
      .replaceAll("&amp;", "&")
  }

  def matchPages(config: Config, sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
    val configFname = config.outputPath + "/conf"
    if ( fs.exists(new Path(configFname)) ) {
      // TODO: Read configuration
    } else {
      config.save(configFname, sqlContext)
    }

    val algFname = config.outputPath + "/alg.json"

    val indexer = udf {(terms: Seq[String]) => hapaxIndex(config.n, terms)}
    val matchMatrix = jaligner.matrix.MatrixGenerator.generate(2, -1)

    val raw = sqlContext.read.format(config.inputFormat).load(config.inputPaths)
    val corpus = PassimApp.testTok(config, raw)
      .select($"id", $"series", datediff($"date", lit("1970-01-01")) as "day",
        $"issue", indexer($"terms") as "index", $"text")
      .withColumn("daybin", ($"day" / config.history).cast("int"))

    val corpus2 = corpus.select($"id" as "pid", $"series" as "pseries",
      $"day" as "pday", $"daybin" as "pdaybin",
      $"issue" as "pissue", $"index" as "pindex", $"text" as "ptext")

    // The predecessor is either in the same history-sized day bin or
    // in the previous one.  The bins are disjoint, so we don't need
    // to dedup the result.
    corpus2
      .withColumn("pdaybin", $"pdaybin" + 1)
      .unionAll(corpus2)
      .join(corpus,
        ($"pseries" === $"series") && ($"pdaybin" === $"daybin")
          && ($"pissue" < $"issue") && (($"pday" + config.history) >= $"day"))
      .select('pid, 'pindex, 'ptext, 'id, 'index, 'text)
      .flatMap((c: Row) => c match {
        case Row(pid: String, pindex: Map[_, _], ptext: String,
          id: String, index: Map[_, _], text: String) => {
          val cs = cleanXML(text)
          val cm = index.asInstanceOf[Map[String, Int]]
          val ps = cleanXML(ptext)
          val pm = pindex.asInstanceOf[Map[String, Int]]
          val inc = PassFun.increasingMatches(pm
            .flatMap(z => if (cm.contains(z._1)) Some((z._2, cm(z._1), 1)) else None))
          val gapped = PassFun.gappedMatches(config.n, config.gap, config.minAlg, inc)
          // println("# rep: " + (pid, id, inc.size, gapped.size))
          if ( inc.size >= config.minRep && gapped.size > 0 ) {
            // TODO: Give high cost to newline mismatches.
            Some(PassFun.alignStrings(config.n * 5, config.gap * 5, matchMatrix,
              (pid, 0, ps.size, ps.size, ps), (id, 0, cs.size, cs.size, cs)))
            // Some(PassAlign(pid, id, "", "", 0, 1, 1, 0, 1, 1, 1, -1))
          } else {
            None
          }
        }
      })
      .toDF
      .write.json(algFname)
      // .write.parquet(algFname)
  }

  def bpSegment(config: Config, sqlContext: SQLContext) = {
    import sqlContext.implicits._

    val fs = FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)

    // sqlContext.read.parquet(algFname)
    //   .withColumnRenamed("id", "aid").drop("alignments")
    //   .join(corpus.drop("terms").drop("uid"), 'aid === 'id, "right_outer")
    //   .explode('id, 'text, 'passageBegin, 'passageEnd, 'termCharEnd)(splitDocs)
    //   .drop("aid").withColumnRenamed("id", "docid").withColumnRenamed("newid", "id")
    //   .drop("text").withColumnRenamed("newtext", "text")
    //   .drop("termCharBegin").drop("termCharEnd")
    //   .drop("termPages").drop("termRegions").drop("termLocs")
    //   .drop("passageBegin").drop("passageEnd").drop("passageLastId")
    //   .write.format(config.outputFormat)
    //   .save(config.outputPath + "/corpus." + config.outputFormat)
  }
}

case class TokText(terms: Array[String], termCharBegin: Array[Int], termCharEnd: Array[Int],
  termPages: Array[String], termRegions: Array[imgCoord], termLocs: Array[String])

object TokApp {
  def tokenize(text: String): TokText = {
    val tok = new passim.TagTokenizer()

    var d = new passim.Document("raw", text)
    tok.tokenize(d)

    val pages = new ArrayBuffer[String]
    val regions = new ArrayBuffer[imgCoord]
    val locs = new ArrayBuffer[String]
    var curPage = ""
    var curCoord = imgCoord(0, 0, 0, 0)
    var curLoc = ""
    var idx = 0
    val p = """^(\d+),(\d+),(\d+),(\d+)$""".r
    for ( t <- d.tags ) {
      val off = t.begin
      while ( idx < off ) {
        pages += curPage
        regions += curCoord
        locs += curLoc
        idx += 1
      }
      if ( t.name == "pb" )
        curPage = t.attributes.getOrElse("n", "")
      else if ( t.name == "loc" )
        curLoc = t.attributes.getOrElse("n", "")
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
        regions += curCoord
        locs += curLoc
        idx += 1
      }
    }

    TokText(d.terms.toSeq.toArray,
      d.termCharBegin.map(_.toInt).toArray,
      d.termCharEnd.map(_.toInt).toArray,
      (if ( curPage == "" ) Array[String]() else pages.toArray),
      (if ( curCoord == imgCoord(0, 0, 0, 0) ) Array[imgCoord]() else regions.toArray),
      (if ( curLoc == "" ) Array[String]() else locs.toArray))
  }
  def tokenizeText(config: Config, raw: DataFrame): DataFrame = {
    val tokenizeCol = udf {(s: String) => tokenize(s)}
    raw.na.drop(Seq(config.id, config.text))
      .withColumn("_tokens", tokenizeCol(col(config.text)))
      .withColumn("terms", col("_tokens")("terms"))
      .withColumn("termCharBegin", col("_tokens")("termCharBegin"))
      .withColumn("termCharEnd", col("_tokens")("termCharEnd"))
      .withColumn("termPages", col("_tokens")("termPages"))
      .withColumn("termRegions", col("_tokens")("termRegions"))
      .withColumn("termLocs", col("_tokens")("termLocs"))
      .drop("_tokens")
    // Used "explode" here, but it behaved badly in spark 1.5.0
  }
}

object PassimApp {
  def hashString(s: String): Long = {
    ByteBuffer.wrap(
      MessageDigest.getInstance("MD5").digest(s.getBytes("UTF-8"))
    ).getLong
  }
  def testTok(config: Config, df: DataFrame): DataFrame = {
    if ( df.columns.contains("terms") ) {
      df
    } else {
      TokApp.tokenizeText(config, df)
    }
  }

  def matchParents(config: Config, sqlContext: SQLContext) {
    import sqlContext.implicits._
    val raw = sqlContext.read.format(config.inputFormat).load(config.inputPaths)
    val corpus = testTok(config, raw)

    val candidates = corpus.select($"cluster" as "p_cluster",
      col(config.id) as "p_id", $"begin" as "p_begin",
      $"terms" as "p_terms", $"date" as "p_date")
    val matchMatrix = jaligner.matrix.MatrixGenerator.generate(2, -1)

    // TODO: Pass through all original fields in raw.
    corpus
      .join(candidates, ($"cluster" === $"p_cluster") && ($"date" > $"p_date"), "left_outer")
      .select("cluster", config.id, "begin", "end", "terms", "date",
        "p_id", "p_begin", "p_terms", "p_date")
      .map({
        case Row(cluster: Long, id: String, begin: Long, end: Long, terms: Seq[_], date:String,
          p_id: String, p_begin: Long, p_terms: Seq[_], p_date: String) => {
          val t = terms.asInstanceOf[Seq[String]].toArray
          val alg = PassFun.alignTerms(config.n, config.gap, matchMatrix,
            p_terms.asInstanceOf[Seq[String]].toArray, t)
          val toklen = t.map(_.size).sum + (t.size - 1)
          ((cluster, id, begin, date),
            ClusterParent(p_id, p_begin, p_date, (alg.matches*1.0f/toklen), alg.score))
        }
        case Row(cluster: Long, id: String, begin: Long, end: Long, terms: Seq[_], date:String,
          _, _, _, _) => {
          ((cluster, id, begin, date), ClusterParent("", 0, "", 0f, 0f))
        }
      })
      .groupByKey
      .map(x => {
        val ((cluster, id, begin, date), parents) = x
        (cluster, id, begin, date,
          parents.filter(_.id != "").toSeq.sortWith(_.score > _.score).toArray)
      })
      .toDF("cluster", config.id, "begin", "date", "parents")
      .orderBy("cluster", "date")
      .write.format(config.outputFormat).save(config.outputPath)
  }

  val hashId = udf {(id: String) => hashString(id)}
  val getPassage = udf { (text: String, begin: Int, end: Int) => text.substring(begin, end) }
  val getLocs = udf {
    (begin: Int, end: Int, termLocs: Seq[String]) =>
    if ( termLocs.size >= end )
      termLocs.toArray.slice(begin, end).distinct.sorted // stable
    else
      Array[String]()
  }
  val getRegions = udf {
    (begin: Int, end: Int, termPages: Seq[String], termRegions: Seq[Row]) =>
    if ( termRegions.size < end )
      Array[imgCoord]()
    else {
      val regions = termRegions.toArray.slice(begin, end)
        .map({ case Row(x: Int, y: Int, w: Int, h: Int) => imgCoord(x, y, w, h) })
        .toArray
      if ( termPages.size < end )
        Array(CorpusFun.boundingBox(regions))
      else
        termPages.slice(begin, end).zip(regions).groupBy(_._1).toIndexedSeq.sortBy(_._1)
          .map(x => CorpusFun.boundingBox(x._2.map(_._2).toArray)).toArray
    }
  }
  def hdfsExists(sc: SparkContext, path: String) = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(sc.hadoopConfiguration)
    val qualified = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    fs.exists(qualified)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Passim Application")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[imgCoord], classOf[Span], classOf[Post],
        classOf[PassAlign], classOf[BoilerPass],
        classOf[TokText], classOf[IdSeries]))
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val parser = new scopt.OptionParser[Config]("passim") {
      opt[String]('M', "mode") action { (x, c) =>
        c.copy(mode = x) } text("Mode: cluster, boilerplate, parents; default=cluster")
      opt[Int]('n', "n") action { (x, c) => c.copy(n = x) } validate { x =>
        if ( x > 0 ) success else failure("n-gram order must be > 0")
      } text("index n-gram features; default=5")
      opt[Int]('h', "history") action { (x, c) => c.copy(history = x) } validate { x =>
        if ( x > 0 ) success else failure("history must be > 0")
      } text("history in days for self reprinting; default=7")
      opt[Int]('u', "maxDF") action { (x, c) =>
        c.copy(maxDF = x) } text("Upper limit on document frequency; default=100")
      opt[Int]('m', "min-match") action { (x, c) =>
        c.copy(minRep = x) } text("Minimum number of n-gram matches between documents; default=5")
      opt[Int]('a', "min-align") action { (x, c) =>
        c.copy(minAlg = x) } text("Minimum length of alignment; default=20")
      opt[Int]('g', "gap") action { (x, c) =>
        c.copy(gap = x) } text("Minimum size of the gap that separates passages; default=100")
      opt[Double]('o', "relative-overlap") action { (x, c) =>
        c.copy(relOver = x) } text("Minimum relative overlap to merge passages; default=0.8")
      opt[Int]('r', "max-repeat") action { (x, c) =>
        c.copy(maxRep = x) } text("Maximum repeat of one series in a cluster; default=10")
      opt[Unit]('p', "pairwise") action { (_, c) =>
        c.copy(pairwise = true) } text("Output pairwise alignments")
      opt[Unit]('d', "docwise") action { (_, c) =>
        c.copy(docwise = true) } text("Output docwise alignments")
      opt[String]('i', "id") action { (x, c) =>
        c.copy(id = x) } text("Field for unique document IDs; default=id")
      opt[String]('t', "text") action { (x, c) =>
        c.copy(text = x) } text("Field for document text; default=text")
      opt[String]('s', "group") action { (x, c) =>
        c.copy(group = x) } text("Field to group documents into series; default=series")
      opt[String]("input-format") action { (x, c) =>
        c.copy(inputFormat = x) } text("Input format; default=json")
      opt[String]("output-format") action { (x, c) =>
        c.copy(outputFormat = x) } text("Output format; default=json")
      opt[Double]('w', "word-length") action { (x, c) => c.copy(wordLength = x)
      } validate { x => if ( x >= 1 ) success else failure("average word length must be >= 1")
      } text("Minimum average word length to match; default=2")
      help("help") text("prints usage text")
      arg[String]("<path>,<path>,...") action { (x, c) =>
        c.copy(inputPaths = x)
      } text("Comma-separated input paths")
      arg[String]("<path>") action { (x, c) =>
        c.copy(outputPath = x) } text("Output path")
    }

    val config = parser.parse(args, Config()) match {
      case Some(c) =>
        c
      case None =>
        sys.exit(-1)
        Config()
    }

    if ( config.mode == "parents" ) {
      matchParents(config, sqlContext)
      sys.exit(0)
    } else if ( config.mode == "boilerplate" ) {
      BoilerApp.matchPages(config, sqlContext)
      sys.exit(0)
    }

    val configFname = config.outputPath + "/conf"
    val indexFname = config.outputPath + "/index.parquet"
    val pairsFname = config.outputPath + "/pairs.parquet"
    val passFname = config.outputPath + "/pass.parquet"
    val clusterFname = config.outputPath + "/clusters.parquet"
    val outFname = config.outputPath + "/out." + config.outputFormat

    if ( hdfsExists(sc, configFname) ) {
      // TODO: Read configuration
    } else {
      config.save(configFname, sqlContext)
    }

    if ( !hdfsExists(sc, outFname) ) {
      val raw = sqlContext.read.format(config.inputFormat).load(config.inputPaths)

      val corpus = testTok(config, raw)
        .withColumn("uid", hashId(col(config.id)))

      if ( !hdfsExists(sc, clusterFname) ) {
        if ( !hdfsExists(sc, passFname) ) {
          val groupCol = if ( raw.columns.contains(config.group) ) config.group else config.id

          val termCorpus = corpus
            .select('uid, 'terms, hashId(corpus(groupCol)).as("gid"))
          // Performance will be awful unless spark.sql.shuffle.partitions is appropriate

          if ( !hdfsExists(sc, pairsFname) ) {

            if ( !hdfsExists(sc, indexFname) ) {
              val minFeatLen: Double = config.wordLength * config.n

              termCorpus
                .flatMap({
                  case Row(uid: Long, terms: Seq[_], gid: Long) => {
                    val md = MessageDigest.getInstance("MD5")
                    terms.asInstanceOf[Seq[String]].sliding(config.n)
                      .zipWithIndex
                      .filter(_._1.map(_.size).sum >= minFeatLen)
                      .map(x => (ByteBuffer.wrap(md.digest(x._1.mkString("~").getBytes("UTF-8")).take(8)).getLong,
                        x._2))
                      .toArray
                      .groupBy(_._1)
                    // Store the count and first posting; could store
                    // some other fixed number of postings.
                      .map { case (feat, post) =>
                        (feat, ArrayBuffer(Post(uid, gid, post.size, post(0)._2))) }
                    }
                })
                .reduceByKey(((a, b) => if (a.size == 0 || b.size == 0 || (a.size + b.size) > config.maxDF ) ArrayBuffer[Post]() else (a ++ b)))
              // Just use plain old document frequency
                .filter(x => x._2.size >= 2)
                .toDF("feat", "docs")
                .write.parquet(indexFname)
            }

            sqlContext.read.parquet(indexFname)
              .select("feat", "docs")
              .flatMap { case Row(f: Long, docs: Seq[_]) => {
                val df = docs.size
                val hapax = docs
                  .map { case Row(uid: Long, gid: Long, tf: Int, post: Int) => (uid, gid, tf, post) }
                  .filter(_._3 == 1).toSeq.sorted.toArray
                val res = new ListBuffer[((Long, Long, Long, Long), (Int, Int, Int))]
                for ( i <- 0 until hapax.size ) {
                  val a = hapax(i)
                  for ( j <- (i + 1) until hapax.size ) {
                    val b = hapax(j)
                    if ( a._2 != b._2 ) {
                      res += (((a._1, a._2, b._1, b._2), (a._4, b._4, df)))
                    }
                  }
                }
                res.toList
              }
            }
              .groupByKey
              .filter(_._2.size >= config.minRep)
              .mapValues(PassFun.increasingMatches)
              .filter(_._2.size >= config.minRep)
              .flatMapValues(PassFun.gappedMatches(config.n, config.gap, config.minAlg, _))
            // Unique IDs will serve as edge IDs in connected component graph
              .zipWithUniqueId
              .flatMap(x => {
                val (((uid1, gid1, uid2, gid2), ((s1, e1), (s2, e2))), mid) = x
                Array((uid1, gid1, s1, e1, mid),
                  (uid2, gid2, s2, e2, mid))
              })
              .toDF("uid", "gid", "begin", "end", "mid")
            // But we need to cache so IDs don't get reassigned.
              .write.parquet(pairsFname)
          }

          val matchMatrix = jaligner.matrix.MatrixGenerator.generate(2, -1)

          // TODO: Should probably be a separate mode.
          if ( config.docwise ) {
            sqlContext.read.parquet(pairsFname)
              .select('uid, 'mid)
              .join(corpus.select('uid, col(config.id), col(config.text)), "uid")
              .map({
                case Row(uid: Long, mid: Long, id: String, text: String) => {
                  (mid, (id, 0, text.size, text.size, text))
                }
              })
              .groupByKey
              .map(x => {
                val docs = x._2.toArray
                PassFun.alignStrings(config.n * 5, config.gap * 5, matchMatrix, docs(0), docs(1))
              })
              .toDF
              .write
              .format(config.outputFormat)
              .save(config.outputPath + "/docs." + config.outputFormat)
          }

          // TODO: Should probably prune document pairs that come from
          // series pairs with inordinate overlap.  Or perhaps that
          // should be an ancillary diagnostic program.
          val align = sqlContext.read.parquet(pairsFname)
            .join(termCorpus, "uid")
            .map({
              case Row(uid: Long, gid: Long, begin: Int, end: Int, mid: Long, terms: Seq[_], gid2: Long) => {
                (mid,
                  PassFun.edgeText(config.gap * 2/3, config.n,
                    IdSeries(uid, gid), terms.asInstanceOf[Seq[String]].toArray, Span(begin, end)))
              }
            })
            .groupByKey
            .flatMap(x => {
              val pass = x._2.toArray
              // The only reason for this array not to have exactly two
              // elements would be erroneous recomputation of the passage
              // pairs.  We've left it unchecked to warn us of errors.
              PassFun.alignEdges(matchMatrix, config.n, config.minAlg, x._1, pass(0), pass(1))
            })

          if ( config.pairwise ) {
            align.cache()
            align
              .map(x => {
                val (doc, (span, mid)) = x
                (doc.id, span.begin, span.end, mid)
              })
              .toDF("uid", "begin", "end", "mid")
              .join(corpus.select('uid, col(config.id), col(config.text),
                'termCharBegin, 'termCharEnd), "uid")
              .map({
                case Row(uid: Long, begin: Int, end: Int, mid: Long,
                  id: String, text: String, termCharBegin: Seq[_], termCharEnd: Seq[_]) => {
                  val tcb = termCharBegin.asInstanceOf[Seq[Int]]
                  val tce = termCharEnd.asInstanceOf[Seq[Int]]

                  (mid, (id, begin, end, tcb.size, text.substring(tcb(begin), tce(end))))
                }
              })
              .groupByKey
              .map(x => {
                val d1 = x._2.head
                val d2 = x._2.last
                PassFun.alignStrings(config.n * 5, config.gap * 5, matchMatrix, d1, d2)
              })
              .toDF
              .write.format(config.outputFormat)
              .save(config.outputPath + "/align." + config.outputFormat)
          }

          val graphParallelism = sc.defaultParallelism

          align
            .groupByKey(graphParallelism)
            .flatMapValues(PassFun.mergeSpans(config.relOver, _))
            .zipWithUniqueId
          // This was getting recomputed on different partitions, thus reassigning IDs.
            .map(v => {
              val ((doc, (span, edges)), id) = v
              (id, doc.id, doc.series, span.begin, span.end, edges)
            })
            .toDF("nid", "uid", "gid", "begin", "end", "edges")
            .write.parquet(passFname)
        }

        val pass = sqlContext.read.parquet(passFname)

        val passNodes = pass.map({
          case Row(nid: Long, uid: Long, gid: Long, begin: Int, end: Int, edges: Seq[_]) =>
            (nid, (IdSeries(uid, gid), Span(begin, end)))
        })
        val passEdges = pass.flatMap({
          case Row(nid: Long, uid: Long, gid: Long, begin: Int, end: Int, edges: Seq[_]) =>
            edges.asInstanceOf[Seq[Long]].map(e => (e, nid))
        })
          .groupByKey
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
            x._2.groupBy(_._1.id).values.groupBy(_.head._1.series).map(_._2.size).max <= config.maxRep
          })
          .flatMap(_._2)
          .map(x => (x._1.id, x._2))
          .groupByKey
          .flatMap(x => x._2.groupBy(_._2).values.flatMap(p => {
            PassFun.mergeSpans(0, p).map(z => (x._1, z._2(0), z._1.begin, z._1.end))
          }))
          .groupBy(_._2)
          .flatMap(x => {
            val size = x._2.size
            x._2.map(p => (p._1, p._2, size, p._3, p._4))
          })
          .toDF("uid", "cluster", "size", "begin", "end")

        clusters.write.parquet(clusterFname)
      }

      val cols = corpus.columns.toSet
      val dateSort = if ( cols.contains("date") ) "date" else config.id

      sqlContext.read.parquet(clusterFname)
        .join(corpus.drop("terms"), "uid")
        .withColumn("pages", getLocs('begin, 'end, 'termPages))
        .withColumn("regions", getRegions('begin, 'end, 'termPages, 'termRegions))
        .withColumn("locs", getLocs('begin, 'end, 'termLocs))
        .drop("termPages").drop("termRegions").drop("termLocs")
        .withColumn("begin", 'termCharBegin('begin))
        .withColumn("end", 'termCharEnd('end))
        .drop("termCharBegin").drop("termCharEnd")
        .withColumn(config.text, getPassage(col(config.text), 'begin, 'end))
        .sort('size.desc, 'cluster, col(dateSort), col(config.id), 'begin)
        .write.format(config.outputFormat).save(outFname)
    }
  }
}
