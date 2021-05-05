import org.apache.spark.sql.functions.{mean, variance}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestApp {
  val Q1_TARGET_NAME = "math score"
  val Q2_TARGET_NAME = "AVG_MATH_SCORE"
  val ALL = "all"

  val GENDER = "GENDER"
  val RACE_ETHNICITY = "RACE_ETHNICITY"
  val PARENTAL_LEVEL_OF_EDUCATION = "PARENTAL_LEVEL_OF_EDUCATION"
  val LUNCH = "LUNCH"
  val TEST_PREPARATION_COURSE = "TEST_PREPARATION_COURSE"

  val ROOT_DATA_PATH = "data/"
  val Q1_SOLVE_RESULT_NAME = "q1_solve_result"
  val Q2_SOLVE_RESULT_NAME = "q2_solve_result"
  val STUDENTS_PERFORMANCE_NAME = "StudentsPerformance.csv"

  val spark: SparkSession = SparkSession.builder.appName("sparkTest")
    .config("spark.master", "local")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  val studentsPerformanceData: DataFrame = getDataFrame(ROOT_DATA_PATH + STUDENTS_PERFORMANCE_NAME)
  val columns = List(GENDER, RACE_ETHNICITY, PARENTAL_LEVEL_OF_EDUCATION, LUNCH, TEST_PREPARATION_COURSE)

  def main(args: Array[String]) {
    solveQ1()
    solveQ2()
    spark.stop()
  }

  def solveQ1(): Unit = {
    val resultDf = studentsPerformanceData
      .withColumnRenamed("gender", GENDER)
      .withColumnRenamed("race/ethnicity", RACE_ETHNICITY)
      .withColumnRenamed("parental level of education", PARENTAL_LEVEL_OF_EDUCATION)
      .withColumnRenamed("lunch", LUNCH)
      .withColumnRenamed("test preparation course", TEST_PREPARATION_COURSE)
      .cube(GENDER, RACE_ETHNICITY, PARENTAL_LEVEL_OF_EDUCATION, LUNCH, TEST_PREPARATION_COURSE)
      .agg(mean(Q1_TARGET_NAME), variance(Q1_TARGET_NAME))
      .na
      .fill(ALL)
      .sort(GENDER, RACE_ETHNICITY, PARENTAL_LEVEL_OF_EDUCATION, LUNCH, TEST_PREPARATION_COURSE)

    createCsvFileFromDataframe(resultDf, Q1_SOLVE_RESULT_NAME)
  }

  def solveQ2(): Unit = {
    import spark.implicits._
    val resultQ1 = getDataFrame(ROOT_DATA_PATH + Q1_SOLVE_RESULT_NAME)
      .withColumnRenamed("avg(math score)", Q2_TARGET_NAME)
      .as[StudentsPerformanceField]
      .filter(row => !isColumnHasAllValue(row))
    columns.foreach(name => {
      createCsvFileFromDataframe(
        calculateMeanTargetGroupByKey(resultQ1.toDF(), name, Q2_TARGET_NAME),
        Q2_SOLVE_RESULT_NAME + "/" + name
      )
    })
  }

  def calculateMeanTargetGroupByKey(df: DataFrame, groupKey: String, target: String): DataFrame = {
    df
      .groupBy(groupKey)
      .mean(target)
  }

  def isColumnHasAllValue(row: StudentsPerformanceField): Boolean = {
    row.GENDER == ALL || row.LUNCH == ALL || row.PARENTAL_LEVEL_OF_EDUCATION == ALL || row.RACE_ETHNICITY == ALL || row.TEST_PREPARATION_COURSE == ALL
  }

  def createCsvFileFromDataframe(dataFrame: DataFrame, dirName: String): Unit = {
    dataFrame
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(ROOT_DATA_PATH + dirName)
  }

  def getDataFrame(path: String): DataFrame = {
    spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(path)
  }

  case class StudentsPerformanceField(
                                       GENDER: String,
                                       RACE_ETHNICITY: String,
                                       PARENTAL_LEVEL_OF_EDUCATION: String,
                                       LUNCH: String,
                                       TEST_PREPARATION_COURSE: String
                                     )

}