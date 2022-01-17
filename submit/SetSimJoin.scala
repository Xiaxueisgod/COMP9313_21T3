package comp9313.proj3

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SetSimJoin {

  def main(args: Array[String]): Unit = {

    val inputFile = args(0)
    val outputFolder = args(1)
    val simThreshold = args(2).toDouble

    // val inputFile = "/Users/kongzeyu/IdeaProjects/COMP9313_21T3/src/main/resources/proj3_input/flickr_london.txt"
    // val outputFolder = "/Users/kongzeyu/IdeaProjects/COMP9313_21T3/src/main/resources/proj3_output/output"
    // val simThreshold = 0.85

    // Spark environment initialization
    val conf = new SparkConf().setAppName("SetSimJoin")
    val sc = new SparkContext(conf)

    val IdExtracted = readFile(sc, inputFile)
    val sortValueListBuffer = valueSorted(IdExtracted)
    // broadcast the simThreshold and sortValueListBuffer
    val broadcast: Broadcast[collection.Map[Int, Int]] = sc.broadcast(sortValueListBuffer)
    val simbroadcast: Broadcast[Double] = sc.broadcast(simThreshold)
    val res = IdExtracted
      // candidates record
      .flatMap(x => prefixTokenRecord(x, broadcast, simbroadcast))
      // Aggregation by key
      // .groupByKey
      .aggregateByKey(ListBuffer[(Int, Array[Int], Int)]())(_ += _, _ ++ _)
      // filter the valid value
      .filter(_._2.length > 1)
      // emit
      .flatMap(x => pairsCompared(x, simbroadcast))
      // Deleting duplicate values
      .reduceByKey((x, _) => x)
      // verify the values
      .mapPartitions(_.map { case (id, (x, y)) => (id, jaccard(x.toSet, y.toSet)) })
      // filter the values which less than simThreshold
      .filter { case (x, sim) => sim >= simbroadcast.value }
    // sort the results
    val resSorted = resultSorted(res)
    val finalResult = concatenateToString(resSorted)
    finalResult.persist()
    // finalResult.foreach(println)
    // println("final result count: " + finalResult.count())
    finalResult.saveAsTextFile(outputFolder)
  }

  def readFile(sc: SparkContext, inputFile: String): RDD[(Int, Array[Int])] = {

    val lines: RDD[Array[String]] = sc.textFile(inputFile, minPartitions = 36).flatMap(line => line.split("\n")).map { line => line.split(" ") }
    val IdExtracted = extractId(lines)
    // IdExtracted.foreach(println)
    return IdExtracted
  }

  def extractId(content: RDD[Array[String]]): RDD[(Int, Array[Int])] = {
    val IdExtracted = content.map(line => {
      val line_length = line.length
      (line(0).toInt, line.slice(1, line_length).map(_.toInt))
    })
    return IdExtracted
  }

  def valueSorted(IdExtracted: RDD[(Int, Array[Int])]): collection.Map[Int, Int] = {
    val sortValueListBuffer = IdExtracted.flatMap(x => x._2.map(t => (t, 1)))
      .reduceByKey((x, y) => x + y)
      .collectAsMap()
    // sortValueListBuffer.foreach(println)
    return sortValueListBuffer
  }

  def prefixTokenRecord(x: (Int, Array[Int]), broadcast: Broadcast[collection.Map[Int, Int]], simbroadcast: Broadcast[Double]): Array[(Int, (Int, Array[Int], Int))] = {

    val recordID = x._1
    val tokens = x._2
    val reorder: Array[Int] = tokens.sortWith((a, b) => {
      val x = broadcast.value.getOrElse(a, -1)
      val y = broadcast.value.getOrElse(b, -1)
      if (x != y) {
        x < y
      } else {
        a < b
      }
    }
    )
    val prefixLen: Double = prefixLengthCalculation(tokens, simbroadcast)
    val res = prefixTokenArrayEmited(reorder, prefixLen, recordID, tokens)
    return res
  }

  def prefixTokenArrayEmited(reorderTokens: Array[Int], prefixLen: Double, recordID: Int, tokens: Array[Int]): Array[(Int, (Int, Array[Int], Int))] = {
    val res = reorderTokens.slice(0, prefixLen.toInt).zipWithIndex.map { case (x, index) => (x, (recordID, tokens, index)) }
    return res
  }

  def prefixLengthCalculation(tokens: Array[Int], simbroadcast: Broadcast[Double]): Double = {
    val prefixLen: Double = tokens.length - Math.ceil(simbroadcast.value * tokens.length) + 1
    return prefixLen
  }

  def pairsCompared(x: (Int, Iterable[(Int, Array[Int], Int)]), simbroadcast: Broadcast[Double]): ListBuffer[((Int, Int), (Array[Int], Array[Int]))] = {

    var res = ListBuffer[((Int, Int), (Array[Int], Array[Int]))]()
    val list = x._2.toList
    for (i <- 0 to list.length - 2) {
      for (j <- i + 1 to list.length - 1) {
        val id1 = list(i)._1
        val id2 = list(j)._1
        val tokens1 = list(i)._2
        val tokens2 = list(j)._2
        val prefix1 = list(i)._3
        val prefix2 = list(j)._3
        res = sizeFiltering(id1, id2, tokens1, tokens2, prefix1, prefix2, simbroadcast, res)
      }
    }
    return res
  }

  def sizeFiltering(id1: Int, id2: Int, tokens1: Array[Int], tokens2: Array[Int], prefix1: Int, prefix2: Int, simbroadcast: Broadcast[Double], res: ListBuffer[((Int, Int), (Array[Int], Array[Int]))]): ListBuffer[((Int, Int), (Array[Int], Array[Int]))] = {

    val alpha = (simbroadcast.value / (1 + simbroadcast.value)) * (tokens1.length + tokens2.length)
    val overlapLeft = (tokens1.slice(0, prefix1) intersect tokens2.slice(0, prefix2)).toSet.size + 1
    val overlapRight = (tokens1.length - prefix1 - 1) min (tokens2.length - prefix2 - 1)
    val pruneCondition = (tokens1.length * 1.0 * simbroadcast.value <= tokens2.length) && (tokens2.length * 1.0 * simbroadcast.value <= tokens1.length) && (overlapLeft + overlapRight) >= alpha
    if (pruneCondition) {
      if (id1 < id2) {
        res.append(((id1, id2), (tokens1, tokens2)))
      } else {
        res.append(((id2, id1), (tokens1, tokens2)))
      }
    }
    return res
  }

  def jaccard(x: Set[Int], y: Set[Int]): Double = {
    val intersectValue = (x & y).size
    val unionValue = (x ++ y).size
    val sim = intersectValue * 1.0 / unionValue
    return sim
  }

  def resultSorted(res: RDD[((Int, Int), Double)]): RDD[((Int, Int), Double)] = {

    return res.sortBy({ case ((x, y), z) => (x, y) }, true)
  }

  def concatenateToString(res: RDD[((Int, Int), Double)]): RDD[String] = {

    val result = res.mapPartitions(_.map { case ((x, y), z) => "(" + x + "," + y + ")" + "\t" + z })
    return result
  }

}
