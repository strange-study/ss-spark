package com.sw.test

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PracticeApp {

  val home: String = "/root/p1"
  val fileName: String = "StudentsPerformance.csv"

  // Original Columns
  // "gender", "race/ethnicity", "parental level of education", "lunch", "test preparation course", "math score", "reading score", "writing score")
  val groups: Seq[String] = Seq("GENDER", "RACE_ETHNICITY", "PARENTAL_L_OF_E", "LUNCH", "TEST_P_C")
  val scores: Seq[String] = Seq("math", "reading", "writing")
  val target = "math"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Practice1 Application").getOrCreate()
    val df = spark.read
      .format("csv")
      .options(Map("header"->"true", "inferSchema"->"true"))
      .load(s"$home/$fileName")
      .toDF(groups ++ scores :_*)

    // Q1
    val q1 = getQ1Result(df)
    writeToCsv(q1, "q1")

    // Q2 (use Q1 Result)
    val q2 = getQ2Result(q1)
    q2.foreach(x => writeToCsv(x._2, s"q2/${x._1}"))

    spark.stop()
  }

  def getQ1Result(df: DataFrame): DataFrame = {
    df.cube(convertToColumnSeq(groups) :_*)
      .agg(mean(target).as("MEAN"), variance(target).as("VARIANCE"))
      .na.fill("ALL", groups)
      .na.fill(0, Seq("VARIANCE")) // 표준 분산 (N=1) 인 경우 => 0
      .sort(convertToColumnSeq(groups) :_*)
  }

  def getQ2Result(df: DataFrame): Map[String, DataFrame] = {
    groups.map(target =>
      target -> {
        df.filter(makeTargetGroupFilterExpr(target))
          .select(target, "MEAN")
          .sort(target)
      }
    ).toMap
  }

  def makeTargetGroupFilterExpr(targetName: String): Column = {
    var filterExpr = col(targetName) =!= "ALL"
    groups.filterNot(str => str == targetName).foreach(str => filterExpr &&= col(str) === "ALL")
    filterExpr
  }

  def writeToCsv(df: DataFrame, name: String): Unit = {
    df.coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(s"$home/$name.csv")
  }

  def convertToColumnSeq(colNames: Seq[String]): Seq[Column] = {
    colNames.map(it => col(it))
  }
}
