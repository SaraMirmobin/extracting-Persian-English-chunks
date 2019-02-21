import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math._
import scala.collection.JavaConversions._
import java.util.ArrayList

import util.control.Breaks._
import org.apache.spark.rdd._
import java.io._

import org.apache.commons.math3.util.MathUtils

object exChunk {
  //////////////////////////////////////////////////mmmmmmmmmmmmmmmmaaaaaaaaaaaaaiiiiiiiiiiiiiiiiiiiiinnnnnnnnnnnnnnnnnnnnnnnnn
  def main(args: Array[String]) {
    println("START")
    //////////////////////////////////////////////////////////////////////////////////////////// 1
    val sc = new SparkContext("local[*]", "exchunks", new SparkConf())
    System.setProperty("file.encoding", "UTF-8")
    val monolingualCorpus = sc.textFile("Path")
    val bilingualCorpus = sc.textFile("Path")
    println("Read corpuses")
    val monolingualCorpusSize = monolingualCorpus.count
    val bilingualCorpusSize = bilingualCorpus.count
    // println("monolingualCorpusSize: " + monolingualCorpusSize)
    // println("bilingualCorpusSize: " + bilingualCorpusSize)
    ////////////////////////////////////////////////////////////////////////////////////////// 2
    val startTimeMillis = System.currentTimeMillis()
    val startTimeMillisEx = System.currentTimeMillis()
    val sentencesChunks = monolingualCorpus
      .flatMap(line => extractionChunkOfSentences(line,2,1))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    println("chunk of sentences extracted")
    // sentencesChunks.foreach(w => println(w))
    ////////////////////////////////////////////////////////////////////////////////////////// 3
    var broadcastedChunks = sc.broadcast(sentencesChunks.collectAsMap())
    println("Broadcast sentencesChunks")
    /////////////////////////////////////////////////////////////////////////////////////////4
    val chunksWithCorrelation = sentencesChunks.filter(_._1.contains("@"))
     //   .filter(t => t._2 != 1)
      .map(t =>findCorrelation( t._1.split("@")(0), t._1.split("@")(1).toInt, t._2 , broadcastedChunks.value, monolingualCorpusSize ) )
    println("chunksWithCorrelation")
  //  chunksWithCorrelation.saveAsTextFile("Path")
    /////////////////////////////////////////////////////////////////////////////////////////// 5
    val filteredChunks = chunksWithCorrelation.map(t => filtering(t)).filter(t => t.size() > 0)
    println("filteredChunks")
    filteredChunks.saveAsTextFile("Path")
    val endTimeMillisEx = System.currentTimeMillis()
    val durationSecondsEx = (endTimeMillisEx - startTimeMillisEx) / 1000
    ////////////////////////////////////////////////////////////////////////////////////////// 6
    val startTimeMillisTr = System.currentTimeMillis()
    val originSentencesChunks = bilingualCorpus
      .flatMap(line => extractionChunkOfSentences(line.split('|')(1),2,1))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    println("originSentencesChunks")
    val destinationSentencesChunks = bilingualCorpus
      .flatMap(line => extractionChunkOfSentences(line.split('|')(0),2,1))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    println("originSentencesChunks")
    ///////////////////////////////////////////////////////////////////////////////////////// 7
    var broadcastedOriginSentencesChunks = sc.broadcast(originSentencesChunks.collectAsMap())
    println("broadcastedOriginSentencesChunks")
    var broadcastedDestinationSentencesChunks = sc.broadcast(destinationSentencesChunks.collectAsMap())
    println("broadcastedDestinationSentencesChunks")
    ////////////////////////////////////////////////////////////////////////////////////////  8
    val chunksWithPossibleTranslation = filteredChunks
      .cartesian(bilingualCorpus)
      .filter(t => isChunkThere(t._1, t._2.split('|')(1)))
      .map(t => (t._1, extractionChunkOfSentences(t._2.split('|')(0),2, 1)))
      .flatMap { case (c, innerList) => innerList.map(c -> _) }
      .filter(t => t._2 != "").filter(t => t._2.contains("@"))
    println("chunksWithPossibleTranslation")
    ///////////////////////////////////////////////////////////////////////////////////////// 9
    val chunksWithTranslation = chunksWithPossibleTranslation
      .map(t => ((t._1, t._2), 1))
      .reduceByKey(_ + _)
      .map(t => (t._1._1, (t._1._2, findCorrelationAgain(t._1._1, t._1._2, t._2, bilingualCorpusSize, broadcastedOriginSentencesChunks.value, broadcastedDestinationSentencesChunks.value)))).groupByKey() //,c10
      .map(t => (t._1(0), t._2.toList.sortBy(r => (r._2, r._1))))
      .filter(t =>t._2.size >5 )
      .map(t => (t._1, t._2(t._2.size-1) ,t._2(t._2.size-2),t._2(t._2.size-3),t._2(t._2.size-4),t._2(t._2.size-5) ))
//      .map(t => (t._1, t._2(0),t._2(1),t._2(2) )) //
    println("chunksWithTranslation")
    chunksWithTranslation.saveAsTextFile("Path")
    val endTimeMillisTr = System.currentTimeMillis()
    val durationSecondsTr = (endTimeMillisTr - startTimeMillisTr) / 1000
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("durationSecondsEx: " + durationSecondsEx)
    println("durationSecondsTr: " + durationSecondsTr)
    println("durationSecondsAll: " + durationSeconds)
    ///////////////////////////////////////////////////////////////////////////////////////// 10
     val groupedChunks = sentencesChunks.filter(_._1.contains("@"))
      .map(t => (t._1.split("@")(0), ((t._1.split("@")(1).toInt), t._2.toLong))).groupByKey()
    val groupedChunkWithCorrelation = groupedChunks.map(t => groupCorrelation(t, broadcastedChunks.value, monolingualCorpusSize))
    val filteredGroupedChunks = groupedChunkWithCorrelation.filter(t => t.size() > 1).map(t => groupFiltering(t)).filter(t => t.size() > 0)
    filteredGroupedChunks.saveAsTextFile("Path")
    println("filteredGroupedChunkWithCorrelation")
    // filteredGroupedChunkWithCorrelation.foreach(t => println(t))
  }

  //end main
  //////////////////////////uuuuuuuuuuuuunnnnnnnnnnnnnniiiiiiiiiiiiiiBBBBBBBBBBBBBBBBiiiiiiiiTTTTTTTTTTTuuuuuuppplllllle
  def extractionChunkOfSentences(args: String, range: Int, distance: Int): ArrayList[String] = {
    val res: ArrayList[String] = new ArrayList[String]()
    var indexx = 0
    val range1 = range - 1
    var word = ""
    for (arg <- args.split(" ")) {
      if (arg != "") {
        res.add(arg)
      }
    }
    val sizeRes = res.size()
    val sizeRes1 = sizeRes - 1
      for (i <- 0 to sizeRes1) {
        breakable {
          for (j <- 2 to range) {
            if (i + j > sizeRes)
              break
            breakable {
              for (y <- 0 to distance) {
                if (i + j + y > sizeRes)
                  break
                var sub = res.subList(i, i + j + y)
                val subSize = sub.size()
                val subSize1 = sub.size() - 1
                if (subSize == j) {
                  for (k <- 0 to subSize1) {
                    if (k != subSize1) {
                      word = word + sub(k) + " "
                    }
                    else {
                      word = word + sub(k)
                    }
                  }
                  indexx = ((j) * 100)
                  word = word + "@" + indexx.toString
                  res.add(word)
                  word = ""
                }
                else if (subSize > j) {
                  val s = subSize - j
                  val subi = sub(0)
                  val subi1 = sub(1)
                  val subend = sub(subSize1)
                  val subend1 = sub(subSize1 - 1)
                  if (j == 2) {
                    word = word + sub(0) + " "
                    word = word + sub(subSize1)
                    indexx = (j * 100) + (s * 10)
                    word = word + "@" + indexx.toString
                    res.add(word)
                    word = ""
                  }
                  else if (j == 3) {
                    word = word + subi + " "
                    word = word + subi1 + " "
                    word = word + subend
                    indexx = (j * 100) + (s * 10)
                    word = word + "@" + indexx.toString
                    res.add(word)
                    word = ""
                    ////////////////////////////
                    word = word + subi + " "
                    word = word + subend1 + " "
                    word = word + subend
                    indexx = (j * 100) + (s * 10) + 1
                    word = word + "@" + indexx.toString
                    res.add(word)
                    word = ""
                  }
                  else if (j == 4) {
                    val subi2 = sub(2)
                    val subend2 = sub(subSize1 - 2)
                    ////////////////////////////////////
                    word = word + subi + " "
                    word = word + subend2 + " "
                    word = word + subend1 + " "
                    word = word + subend
                    indexx = (j * 100) + (s * 10)
                    word = word + "@" + indexx.toString
                    res.add(word)
                    word = ""
                    /////////////////////////////////////
                    word = word + subi + " "
                    word = word + subi1 + " "
                    word = word + subend1 + " "
                    word = word + subend
                    indexx = (j * 100) + (s * 10) + 1
                    word = word + "@" + indexx.toString
                    res.add(word)
                    word = ""
                    /////////////////////////////////////
                    word = word + subi + " "
                    word = word + subi1 + " "
                    word = word + subi2 + " "
                    word = word + subend
                    indexx = (j * 100) + (s * 10) + 2
                    word = word + "@" + indexx.toString
                    res.add(word)
                    word = ""
                    indexx = (j * 100) + (s * 10) + 2
                    //////////////////////////////////////
                  }
                }
              }
            }
          }
        }
      }
   // }
    return res
  }

  //////////////////////////////////////////////////fffffffffffiiiiiiiiinddddddddddddddddddddddddddXXXXXXXXXXXXXXX22222
  def groupCorrelation(gruopTuple: (String, Iterable[(Int, Long)]), fullMap: scala.collection.Map[String, Int], all: Long): ArrayList[String] = {
    val res: ArrayList[String] = new ArrayList[String]()
    val tuple2 = gruopTuple._1.toString //chunk
    //  println("chunk: "+ tuple2)
    val myIterable = gruopTuple._2.toStream
    val sizeMyIterable = myIterable.size()
    var word = ""
    var n111: Long = 0
    var n100: Long = 0
    var n011: Long = 0
    var n000: Long = 0
    var types = ""
    for (i <- 0 to sizeMyIterable - 1) {
      types = types + "#" + myIterable.get(i)._1.toString
    }
    for (i <- 0 to sizeMyIterable - 1) {
      val tuple3 = myIterable.get(i)._1.toLong // type
      var n11 = myIterable.get(i)._2.toLong // n11
      if (n11 > all) {
        n11 = all
      }
      n111 = n111 + n11
      if (tuple3 == 200 | tuple3 == 210 | tuple3 == 220 | tuple3 == 230 | tuple3 == 240) {
        //   println("n11= "+ n11)
        var n10 = fullMap.get(tuple2.split(" ")(0)).getOrElse(0) - n11
        if (n10 > all) {
          n10 = all - n11
        }
        //  println("n10= "+ n10)
        var n01 = fullMap.get(tuple2.split(" ")(1)).getOrElse(0) - n11
        if (n01 > all) {
          n01 = all - n11
        }
        //   println("n01= "+ n01)
        //    println("all= "+ all)
        val n00 = all - n10 - n01 - n11
        //    println("n00= "+ n00)
        n100 = n100 + n10
        n011 = n011 + n01
        n000 = n000 + n00
        val x2 = correlation(n11, n10, n01, n00)
        // println( tuple2.toString + " , " + tuple3.toString  + " => "  + " n11: " + n11 + ", n10: " + n10 + ", n01: "+ n01 + ", n00: " + n00 + " x2: "+ x2 )
        word = tuple2.toString + "@" + x2.toString + "@" + "1" + "@" + tuple3.toString
        res.add(word)
        word = ""
      }
      else if (tuple3 == 300 | tuple3 == 311 | tuple3 == 321 | tuple3 == 331 | tuple3 == 341) {
        val s1 = tuple2.split(" ")(0)
        val s2 = tuple2.split(" ")(1) + " " + tuple2.split(" ")(2) + "@" + 200.toString
        var n10 = fullMap.get(s1).getOrElse(0) - n11
        if (n10 > all) {
          n10 = all - n11
        }
        var n01 = fullMap.get(s2).getOrElse(0) - n11
        if (n01 > all) {
          n01 = all - n11
        }
        val n00 = all - n10 - n01 - n11
        n100 = n100 + n10
        n011 = n011 + n01
        n000 = n000 + n00
        val x2 = correlation(n11, n10, n01, n00)
        //  println( tuple2.toString + " , " + tuple3.toString  + " => "  + " n11: " + n11 + ", n10: " + n10 + ", n01: "+ n01 + ", n00: " + n00 + " x2: " + x2 )
        word = tuple2.toString + "@" + x2.toString + "@" + "1" + "@" + tuple3.toString
        res.add(word)
        word = ""
      }
      else if (tuple3 == 310 | tuple3 == 320 | tuple3 == 330 | tuple3 == 340) {
        val s1 = tuple2.split(" ")(0) + " " + tuple2.split(" ")(1) + "@" + 200.toString
        val s2 = tuple2.split(" ")(2)
        var n10 = fullMap.get(s1).getOrElse(0) - n11
        if (n10 > all) {
          n10 = all - n11
        }
        var n01 = fullMap.get(s2).getOrElse(0) - n11
        if (n01 > all) {
          n01 = all - n11
        }
        val n00 = all - n10 - n01 - n11
        n100 = n100 + n10
        n011 = n011 + n01
        n000 = n000 + n00
        val x2 = correlation(n11, n10, n01, n00)
        //println( tuple2.toString + " , " + tuple3.toString  + " => "  + " n11: " + n11 + ", n10: " + n10 + ", n01: "+ n01 + ", n00: " + n00 + " x2: " + x2 )
        word = tuple2.toString + "@" + x2.toString + "@" + "1" + "@" + tuple3.toString
        res.add(word)
        word = ""
      }
      else if (tuple3 == 400 | tuple3 == 410 | tuple3 == 420 | tuple3 == 430 | tuple3 == 440) {
        val s1 = tuple2.split(" ")(0)
        val s2 = tuple2.split(" ")(1) + " " + tuple2.split(" ")(2) + " " + tuple2.split(" ")(3) + "@" + 300.toString
        var n10 = fullMap.get(s1).getOrElse(0) - n11
        if (n10 > all) {
          n10 = all - n11
        }
        var n01 = fullMap.get(s2).getOrElse(0) - n11
        if (n01 > all) {
          n01 = all - n11
        }
        val n00 = all - n10 - n01 - n11
        n100 = n100 + n10
        n011 = n011 + n01
        n000 = n000 + n00
        val x2 = correlation(n11, n10, n01, n00)
        // println( tuple2.toString + " , " + tuple3.toString  + " => "  + " n11: " + n11 + ", n10: " + n10 + ", n01: "+ n01 + ", n00: " + n00 + " x2: " + x2)

        word = tuple2.toString + "@" + x2.toString + "@" + "1" + "@" + tuple3.toString
        res.add(word)
        word = ""
      }
      else if (tuple3 == 411 | tuple3 == 421 | tuple3 == 431 | tuple3 == 441) {
        val s1 = tuple2.split(" ")(0) + " " + tuple2.split(" ")(1) + "@" + 200.toString
        val s2 = tuple2.split(" ")(2) + " " + tuple2.split(" ")(3) + "@" + 200.toString
        var n10 = fullMap.get(s1).getOrElse(0) - n11
        if (n10 > all) {
          n10 = all - n11
        }
        var n01 = fullMap.get(s2).getOrElse(0) - n11
        if (n01 > all) {
          n01 = all - n11
        }
        val n00 = all - n10 - n01 - n11
        n100 = n100 + n10
        n011 = n011 + n01
        n000 = n000 + n00
        val x2 = correlation(n11, n10, n01, n00)
        // println( tuple2.toString + " , " + tuple3.toString  + " => "  + " n11: " + n11 + ", n10: " + n10 + ", n01: "+ n01 + ", n00: " + n00 +" x2: " + x2)
        //+ ", s1: " +s2 + ", fullMap.get(s2).getOrElse(0): " + (fullMap.get(s2).getOrElse(0))
        word = tuple2.toString + "@" + x2.toString + "@" + "1" + "@" + tuple3.toString
        res.add(word)
        word = ""
      }
      else {
        //if (tuple3 == 412 | tuple3 == 422 | tuple3 == 432 | tuple3 == 442)
        val s1 = tuple2.split(" ")(0) + " " + tuple2.split(" ")(1) + " " + tuple2.split(" ")(2) + "@" + 300.toString
        val s2 = tuple2.split(" ")(3)
        var n10 = fullMap.get(s1).getOrElse(0) - n11
        if (n10 > all) {
          n10 = all - n11
        }
        var n01 = fullMap.get(s2).getOrElse(0) - n11
        if (n01 > all) {
          n01 = all - n11
        }
        val n00 = all - n10 - n01 - n11
        n100 = n100 + n10
        n011 = n011 + n01
        n000 = n000 + n00
        val x2 = correlation(n11, n10, n01, n00)
        // println( tuple2.toString + " , " + tuple3.toString  + " => "  + " n11: " + n11 + ", n10: " + n10 + ", n01: "+ n01 + ", n00: " + n00 + " x2: " + x2 )
        word = tuple2.toString + "@" + x2.toString + "@" + "1" + "@" + tuple3.toString
        res.add(word)
        word = ""
      }
    }
    val sizeRes = res.size()
    if (sizeRes > 1) {
      var tmp: Float = correlation(n111, n100, n011, n000)
      word = tuple2.toString + "@" + tmp.toString + "@" + "2" + "@" + types
      res.add(word)
      word = ""
    }
    return res
  }
  ////////////////////////////////////////////cccccccccchhhhhhhhhhiiiiiiiiiii____ssssssqqqqqqqquuuuuuuuuaaaaarrrreeeeee
  def correlation(n11: Long, n10: Long, n01: Long, n00: Long): Float = {

    var tmp: Float =    n11 + n10 + n01 + n00
    tmp = tmp * ( (n11 * n00) - (n10 * n01) ) * ( (n11 * n00) - (n10 * n01) )
    if (((n11 + n10) * (n01 + n00) * (n11 + n01) * (n10 + n00)) != 0) {
      tmp = (tmp) / ((n11 + n10) * (n01 + n00) * (n11 + n01) * (n10 + n00))
    }
    else {
      tmp = 20
    }
    return tmp
  }
  ////////////////////////////////ffffffffffiiiiiiiilllllllllllltttttteeeeeeeerrrrrrrrriiiiiiiiiinnnnnnnnnnnnngggggggg222
  def groupFiltering(result12: ArrayList[String]): ArrayList[String] = {
    val res: ArrayList[String] = new ArrayList[String]()
    val sizeRes1 = result12.size()-1
    if (result12(sizeRes1).split("@")(1).toFloat < 6.63) {
      for (i <- 0 to sizeRes1) {
        res.add(result12(i))
      }
    }
    return res
  }
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  def groupFiltering2(result12: ArrayList[String]): ArrayList[String] = {
    val res: ArrayList[String] = new ArrayList[String]()
    val sizeRes1 = result12.size()-1
    if (result12(sizeRes1).split("@")(1).toFloat < 6.63) {
      for (i <- 0 to sizeRes1) {
        res.add(result12(i))
      }
    }
    return res
  }
  //////////////////////////////////////iiiiiissssssssCCCCCChhhhhhhhuuuuunnnnnnnnnkkkkkkkTTTThhhheeeeeerrrrrrrrrrrreeeeeeeee
  def isChunkThere(chunk: ArrayList[String], sentence: String): Boolean = {
    //println("isChunkThere ")
    // println(chunk(0).split("@")(0) + " -> " + sentence)
    if (chunk(0).split("@")(1).toInt == 200 | chunk(0).split("@")(1).toInt == 300 | chunk(0).split("@")(1).toInt == 400) {
      if (sentence.contains(chunk(0).split("@")(0))) {
        return true
      }
      else {
        return false
      }
    }
    else {
      val res: ArrayList[String] = new ArrayList[String]()
      val res1: ArrayList[String] = new ArrayList[String]()
      for (arg <- sentence.split(" ")) {
        if (arg != "") {
          res.add(arg)
        }
      }
      val t = (chunk(0).split("@")(1).toInt) / 100
      if (t > res.size()) {
        return false
      }
      else {
        val a = chunk(0).split("@")(1).toInt
        var word = ""
        if (a == 210) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 3 > sizeRes)
                break
              var sub = res.subList(i, i + 2 + 1) //
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          val m = res1.size
          val n = m - 1
          for (j <- 0 to n) {
            var u = res1(j)
            var h = chunk(0).split("@")(0)
            if (u == h) {
              return true
            }
          }
          return false
        }
        else if (a == 220) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 4 > sizeRes)
                break
              var sub = res.subList(i, i + 2 + 2)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 230) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 5 > sizeRes)
                break
              var sub = res.subList(i, i + 2 + 3)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 240) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 6 > sizeRes)
                break
              var sub = res.subList(i, i + 2 + 4)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 310) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 4 > sizeRes)
                break
              var sub = res.subList(i, i + 4)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 311) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 4 > sizeRes)
                break
              var sub = res.subList(i, i + 4)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 320) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 5 > sizeRes)
                break
              var sub = res.subList(i, i + 5)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 321) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 5 > sizeRes)
                break
              var sub = res.subList(i, i + 5)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 330) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 6 > sizeRes)
                break
              var sub = res.subList(i, i + 6)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 331) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 6 > sizeRes)
                break
              var sub = res.subList(i, i + 6)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 340) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 7 > sizeRes)
                break
              var sub = res.subList(i, i + 7)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 341) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 7 > sizeRes)
                break
              var sub = res.subList(i, i + 7)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 410) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 5 > sizeRes)
                break
              var sub = res.subList(i, i + 5)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 2) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 411) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 5 > sizeRes)
                break
              var sub = res.subList(i, i + 5)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 412) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 5 > sizeRes)
                break
              var sub = res.subList(i, i + 5)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(2) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 420) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 6 > sizeRes)
                break
              var sub = res.subList(i, i + 6)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 2) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 421) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 6 > sizeRes)
                break
              var sub = res.subList(i, i + 6)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 422) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 6 > sizeRes)
                break
              var sub = res.subList(i, i + 6)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(2) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 430) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 7 > sizeRes)
                break
              var sub = res.subList(i, i + 7)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 2) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 431) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 7 > sizeRes)
                break
              var sub = res.subList(i, i + 7)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 432) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 7 > sizeRes)
                break
              var sub = res.subList(i, i + 7)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(2) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              //  println("true 992")
              return true
            }
          }
          //*********************
          return false
        }
        else if (a == 440) {
          //*********************
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 8 > sizeRes)
                break
              var sub = res.subList(i, i + 8)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(subSize1 - 2) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else if (a == 441) {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 8 > sizeRes)
                break
              var sub = res.subList(i, i + 8)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(subSize1 - 1) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
        else {
          val sizeRes = res.size()
          val sizeRes1 = sizeRes - 1
          for (i <- 0 to sizeRes1) {
            breakable {
              if (i + 8 > sizeRes)
                break
              var sub = res.subList(i, i + 8)
              val subSize1 = sub.size() - 1
              word = word + sub(0) + " "
              word = word + sub(1) + " "
              word = word + sub(2) + " "
              word = word + sub(subSize1)
              res1.add(word)
              word = ""
            }
          }
          for (i <- 0 to res1.size() - 1) {
            if (res1(i) == chunk(0).split("@")(0)) {
              return true
            }
          }
          return false
        }
      }
    }
  }
  ////////////////////////////////fffffffiiiiiiiiinnnnnnnnndddddddXXXXXXXXXXX2222222222AAAAAAAAAggggggggggaaaaaaaaiiiiiiiinnnn
  def findCorrelationAgain(tuple1: ArrayList[String], tuple2: String, c11: Int, c00: Long, fullMap: scala.collection.Map[String, Int], fullMap2: scala.collection.Map[String, Int]): Float = {
    // , c10: (String,Int)
    val n11 = c11
    var tuple11 = tuple1(0).split("@")(0) + "@" + tuple1(0).split("@")(1)
    val n10 = fullMap.get(tuple11).getOrElse(0) - n11
    val n01 = fullMap2.get(tuple2).getOrElse(0) - n11
    val n00 = c00 - n10 - n01 - n11
    return correlation(n11, n10, n01, n00)
  }
  ////////////////////////////////////////////////wwwwwwhhhhiiiittthhhooouuutttGGGrrrooouuuppp
  def findCorrelation(aChunk:String ,typee: Int ,c11:Int ,fullMap: scala.collection.Map[String, Int], all: Long ): ArrayList[String] = {
    val res: ArrayList[String] = new ArrayList[String]()
    val tuple2 = aChunk //chunk
    var word = ""
    val tuple3 = typee.toLong // type
    var n11 = c11.toLong // n11
    if (n11 > all) {
      n11 = all
    }
    if (tuple3 == 200 | tuple3 == 210 | tuple3 == 220 | tuple3 == 230 | tuple3 == 240) {
      //   println("n11= "+ n11)
      var n10 = fullMap.get(tuple2.split(" ")(0)).getOrElse(0) - n11
      if (n10 > all) {
        n10 = all - n11
      }
      //  println("n10= "+ n10)
      var n01 = fullMap.get(tuple2.split(" ")(1)).getOrElse(0) - n11
      if (n01 > all) {
        n01 = all - n11
      }
      val n00 = all - n10 - n01 - n11
      val x2 = correlation(n11, n10, n01, n00)
      word = tuple2.toString + "@" +tuple3.toString  + "@" +  x2.toString
      res.add(word)
      word = ""
    }
    else if (tuple3 == 300 | tuple3 == 311 | tuple3 == 321 | tuple3 == 331 | tuple3 == 341) {
      val s1 = tuple2.split(" ")(0)
      val s2 = tuple2.split(" ")(1) + " " + tuple2.split(" ")(2) + "@" + 200.toString
      var n10 = fullMap.get(s1).getOrElse(0) - n11
      if (n10 > all) {
        n10 = all - n11
      }
      var n01 = fullMap.get(s2).getOrElse(0) - n11
      if (n01 > all) {
        n01 = all - n11
      }
      val n00 = all - n10 - n01 - n11
      val x2 = correlation(n11, n10, n01, n00)
      word = tuple2.toString + "@" +tuple3.toString  + "@" +  x2.toString
      res.add(word)
      word = ""
    }
    else if (tuple3 == 310 | tuple3 == 320 | tuple3 == 330 | tuple3 == 340) {
      val s1 = tuple2.split(" ")(0) + " " + tuple2.split(" ")(1) + "@" + 200.toString
      val s2 = tuple2.split(" ")(2)
      var n10 = fullMap.get(s1).getOrElse(0) - n11
      if (n10 > all) {
        n10 = all - n11
      }
      var n01 = fullMap.get(s2).getOrElse(0) - n11
      if (n01 > all) {
        n01 = all - n11
      }
      val n00 = all - n10 - n01 - n11
      val x2 = correlation(n11, n10, n01, n00)
      word = tuple2.toString + "@" +tuple3.toString  + "@" +  x2.toString
      res.add(word)
      word = ""
    }
    else if (tuple3 == 400 | tuple3 == 410 | tuple3 == 420 | tuple3 == 430 | tuple3 == 440) {
      val s1 = tuple2.split(" ")(0)
      val s2 = tuple2.split(" ")(1) + " " + tuple2.split(" ")(2) + " " + tuple2.split(" ")(3) + "@" + 300.toString
      var n10 = fullMap.get(s1).getOrElse(0) - n11
      if (n10 > all) {
        n10 = all - n11
      }
      var n01 = fullMap.get(s2).getOrElse(0) - n11
      if (n01 > all) {
        n01 = all - n11
      }
      val n00 = all - n10 - n01 - n11
      val x2 = correlation(n11, n10, n01, n00)
      word = tuple2.toString + "@" +tuple3.toString  + "@" +  x2.toString
      res.add(word)
      word = ""
    }
    else if (tuple3 == 411 | tuple3 == 421 | tuple3 == 431 | tuple3 == 441) {
      val s1 = tuple2.split(" ")(0) + " " + tuple2.split(" ")(1) + "@" + 200.toString
      val s2 = tuple2.split(" ")(2) + " " + tuple2.split(" ")(3) + "@" + 200.toString
      var n10 = fullMap.get(s1).getOrElse(0) - n11
      if (n10 > all) {
        n10 = all - n11
      }
      var n01 = fullMap.get(s2).getOrElse(0) - n11
      if (n01 > all) {
        n01 = all - n11
      }
      val n00 = all - n10 - n01 - n11
      val x2 = correlation(n11, n10, n01, n00)
      word = tuple2.toString + "@" +tuple3.toString  + "@" +  x2.toString
      res.add(word)
      word = ""
    }
    else {
      val s1 = tuple2.split(" ")(0) + " " + tuple2.split(" ")(1) + " " + tuple2.split(" ")(2) + "@" + 300.toString
      val s2 = tuple2.split(" ")(3)
      var n10 = fullMap.get(s1).getOrElse(0) - n11
      if (n10 > all) {
        n10 = all - n11
      }
      var n01 = fullMap.get(s2).getOrElse(0) - n11
      if (n01 > all) {
        n01 = all - n11
      }
      val n00 = all - n10 - n01 - n11
      val x2 = correlation(n11, n10, n01, n00)
      word = tuple2.toString + "@" +tuple3.toString  + "@" +  x2.toString
      res.add(word)
      word = ""
    }
    return res
  }
  //////////////////////////////////////////////fffffffiiiiiilllllllttteeerrrriiinnnngggg
  def filtering(result12: ArrayList[String]): ArrayList[String] = {
    val res: ArrayList[String] = new ArrayList[String]()
    if (result12(0).split("@")(2).toFloat < 7 )
      res.add(result12(0))
    return res
  }
  /////////////////////////////////////////////////////////////////////////////////////
}

// end object