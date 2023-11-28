import com.datastax.oss.driver.api.core.CqlSession
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{asc, count, desc, col}
import java.net.InetSocketAddress
import scala.jdk.CollectionConverters._

object SparkManipulations extends App {
  ///////////////////////////////// Create a connection with Cassandra DB /////////////////////////////////////
  val session = CqlSession.builder()
    .addContactPoint(new InetSocketAddress("localhost", 9042))
    .withLocalDatacenter("datacenter1")
    .build()

  // Use the studentsLog keyspace.
  session.execute("USE studentsLog")

  // Select all fields from the table.
  private val selectStatement =
    """
      |SELECT * FROM ce_students
      |""".stripMargin

  // Execute the select query and process the result.
  private val resultSet = session.execute(selectStatement)
  resultSet.forEach { row =>
    // Retrieve and process each field.
    val id = row.getInt("id")
    val yearOfStudy = row.getInt("year_of_study")
    val age = row.getInt("age")
    val attendance = row.getInt("attendance")
    val children = row.getInt("children")
    val city = row.getString("city")
    val finishedCredits = row.getInt("finished_credits")
    val gpa = row.getFloat("gpa")
    val grades = row.getMap[String, String]("grades", classOf[String], classOf[String]).asScala.toMap
    val healthConditions = row.getSet[String]("health_conditions", classOf[String]).asScala.toList
    val income = row.getInt("income")
    val languages = row.getSet[String]("languages", classOf[String]).asScala.toSet
    val name = row.getString("name")
    val scores = row.getMap[String, Integer]("scores", classOf[String], classOf[Integer]).asScala.toSeq
    val siblings = row.getInt("siblings")
    val studentExpenses = row.getMap[String, Integer]("student_expenses", classOf[String], classOf[Integer]).asScala.toMap

    // Print the results to confirm the proper functioning of data retrieval from Cassandra.
    println(s"ID: $id")
    println(s"Year Of Study: $yearOfStudy")
    println(s"Age: $age")
    println(s"Attendance: $attendance")
    println(s"Children: $children")
    println(s"City: $city")
    println(s"Finished Credits: $finishedCredits")
    println(s"GPA: $gpa")
    println(s"Grades: $grades")
    println(s"Health Conditions: $healthConditions")
    println(s"Income: $income")
    println(s"Languages: $languages")
    println(s"Name: $name")
    println(s"Scores: $scores")
    println(s"Siblings: $siblings")
    println(s"Student Expenses: $studentExpenses")
    println()
  }

  // Create a spark session.
  private val spark = SparkSession.builder() // To create a new SparkSession.
    .appName("CassandraDataLoading")
    .master("local")
    .getOrCreate()

  // Load Data from Cassandra into DataFrame:
  private val cassandraTable = "ce_students"
  private val keyspace = "studentslog"

  private val CEStudentsDF = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> cassandraTable, "keyspace" -> keyspace))
    .load()
  CEStudentsDF.show()

  // Show the schema of the DataFrame.
  CEStudentsDF.printSchema()

  // Choose the students to be included in the honor list.
  private val honorListDF = CEStudentsDF.select("id", "name", "year_of_study", "gpa").filter("gpa > 3.5").orderBy(desc("gpa"))
  honorListDF.show()

  // Define honor list table.
  private var createTable =
    """
      | CREATE TABLE IF NOT EXISTS honor_list (
      |  id INT PRIMARY KEY,
      |  name TEXT,
      |  year_of_study INT,
      |  gpa FLOAT
      |)
    """.stripMargin

  // Execute the Cassandra creation query.
  session.execute(createTable)

  // Save honorListDF to Cassandra honor_list table.
  honorListDF.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "honor_list", "keyspace" -> keyspace))
    .mode(SaveMode.Append)
    .option("confirm.truncate", value = true)
    .save()

  // List of students who have successfully passed.
  private val passedStudents = CEStudentsDF.select("id", "name", "year_of_study", "gpa").filter("gpa >= 1.5").orderBy(asc("gpa"))
  passedStudents.show()

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS passed_students (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study TEXT,
      | gpa INT
      | )
      |""".stripMargin

  session.execute(createTable)

  passedStudents.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "passed_students", "keyspace" -> keyspace))
    .mode(SaveMode.Append)
    .option("confirm.truncate", value = true)
    .save()

  // List of students who have Failed.
  private val failedStudents = CEStudentsDF.select("id", "name", "year_of_study", "gpa").filter("gpa < 1.5")
  failedStudents.show()

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS failed_students (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study INT,
      | gpa INT
      | )
      |""".stripMargin

  session.execute(createTable)

  failedStudents.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "failed_students", "keyspace" -> keyspace))
    .mode(SaveMode.Append)
    .option("confirm.truncate", value = true)
    .save()

  // List of students who have been awarded a 33% scholarship.
  private val scholarship1Recipients = CEStudentsDF.select("id", "name", "year_of_study", "gpa").filter("gpa >= 3.5 AND gpa <= 3.64").orderBy(desc("gpa"))
  scholarship1Recipients.show()

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS scholarship1_recipients (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study INT,
      | gpa INT
      | )
      |""".stripMargin
  session.execute(createTable)

  scholarship1Recipients.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "scholarship1_recipients", "keyspace" -> keyspace))
    .mode(SaveMode.Append)
    .option("confirm.truncate", value = true)
    .save()

  // List of students who have been awarded a 50% scholarship
  private val scholarship2Recipients = CEStudentsDF.select("id", "name", "year_of_study", "gpa").filter("gpa > 3.64").orderBy(desc("gpa"))
  scholarship2Recipients.show()

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS scholarship2_recipients (
      | id INT PRIMARY KEY,
      | name TEXT,
      | year_of_study INT,
      | gpa INT
      | )
      |""".stripMargin

  session.execute(createTable)

  scholarship2Recipients.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "scholarship2_recipients", "keyspace" -> keyspace))
    .mode(SaveMode.Append)
    .option("confirm.truncate", value = true)
    .save()

  private val studentsPerYearDF = CEStudentsDF.groupBy("year_of_study")
    .agg(count("*") as "students_count")
    .orderBy(col("year_of_study"))
  studentsPerYearDF.show()

  createTable =
    """
      | CREATE TABLE IF NOT EXISTS students_per_academic_year (
      | year_of_study INT PRIMARY KEY,
      | students_count INT
      | )
      |""".stripMargin

  session.execute(createTable)

  studentsPerYearDF.write
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "students_per_academic_year", "keyspace" -> keyspace))
    .mode(SaveMode.Append)
    .option("confirm.truncate", value = true)
    .save()

  spark.close()
  session.close()
}