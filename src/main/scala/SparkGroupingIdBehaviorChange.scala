package dev.mprice

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkGroupingIdBehaviorChange {

  def main(args: Array[String]): Unit = {
    implicit val spark = org.apache.spark.sql.SparkSession.builder().master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    new GroupingSetBugTest().runTest()
  }

}

class GroupingSetBugTest(implicit spark: SparkSession) {

  def runTest() = {

    val matchingOrderGroupingSets = """
GROUPING SETS (
    (col1),
    (col2, col3)
)                 
"""

    val nonMatchingOrderGroupingSets = """
GROUPING SETS (
    (col2, col3),
    (col1)
)                 
"""

    val groupingSQL = """
SELECT 'col1' as col1,
       'col2' as col2,
       'col3' as col3,
       grouping_id()            as grouping_id,
       count(1)                 as rowCount
from __THIS__
GROUP BY col1, col2, col3
"""

    testSQL("Grouping sets in same order as group by", groupingSQL ++ matchingOrderGroupingSets)
    testSQL("Grouping sets in different order as group by", groupingSQL ++ nonMatchingOrderGroupingSets)
  }

  def testSQL(testName: String, sql: String): Unit = {
    println("----------------------")
    println(s"Start test: $testName")

    val df = spark.range(1)
    df.createOrReplaceTempView("__THIS__")

    println(sql)

    val groupDF = spark.sql(sql)

    //    groupDF.printSchema()
    groupDF.orderBy("grouping_id").show(1000, 1000)

    //    validateNumberOfGroupingSets(sql, groupDF)

    getGroupingSetDimensions(groupDF).foreach {
      case (bitmap: Long, dimensions: Array[String]) =>
        println(s"Grouping bitmap and associated dimensions: $bitmap ${dimensions.mkString(", ")}")
    }

    println(s"End test: $testName")
  }

  // This converts the grouping_id into a bitmap corresponding to which dimensions are *present* in the data,
  // from first to last.

  def flipAndMirrorBits(bitmap: Long, bitmapLength: Int): Long = {
    // Mirror the bits: Left <-> Right
    var output = 0L
    for (i <- 0 until bitmapLength) {
      if ((bitmap & (1 << i)) != 0) {
        output = output | (1 << bitmapLength - i - 1)
      }
    }
    // Flip the bits. 0 <-> 1
    output ^ ((1 << bitmapLength) - 1)
  }

  // Put all dimensions before the grouping_id
  def getNumDimensions(df: DataFrame) = df.schema.fields.map(_.name).indexOf("grouping_id")

  def getGroupDimensions(df: DataFrame, bitmap: Long): Array[String] = {
    val numDimensions = getNumDimensions(df)
    val dimensions = df.schema.fields.take(numDimensions)
    dimensions.zipWithIndex
      .filter {
        case (_, i) => (flipAndMirrorBits(bitmap, numDimensions) & (1 << i)) != 0
      }
      .map(_._1.name)
  }
  def getGroupingSetDimensions(df: DataFrame)(implicit spark: SparkSession): Array[(Long, Array[String])] = {
    import spark.implicits._

    val groupingIds = df.select("grouping_id").distinct.as[Long].collect.sorted

    if (groupingIds.isEmpty) {
      df.show(100, 100)
      throw new RuntimeException(s"No group sets found during group set discovery reading from Dataframe: ${df.schema}")
    }

    groupingIds
      .map { bitmap: Long => bitmap -> getGroupDimensions(df, bitmap) }
      .sortBy(_._1)
  }

  // This is for testing a different issue involving a trailing extra "," in the GROUP BY clause.
  def validateNumberOfGroupingSets(sql: String, groupDF: DataFrame) = {
    var numGroupingSets = 0
    var inGroupingSets = false
    sql.split(raw"\n").foreach { l =>
      if ("GROUPING SETS".r.findFirstIn(l).isDefined) {
        inGroupingSets = true
      } else {
        if (inGroupingSets && raw"^\s+\(".r.findFirstIn(l).isDefined) {
          numGroupingSets += 1
        }
      }
    }
    val groupingIdDF = groupDF.groupBy("grouping_id").count.orderBy("grouping_id")
    groupingIdDF.show(1000, 1000)
//    groupDF.explain(true)
    println(s"Grouping Id Count ${groupingIdDF.count} == Num Grouping Sets ${numGroupingSets}")
    assert(groupingIdDF.count == numGroupingSets)
  }
}
