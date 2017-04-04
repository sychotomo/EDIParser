/**
  * Created by abhishek on 30/3/17.
  */
import com.databricks.spark.xml.XmlReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.berryworks.edireader.EDIReader
import com.berryworks.edireader.EDIReaderFactory
import com.berryworks.edireader.EDISyntaxException
import com.berryworks.edireader.error.EDISyntaxExceptionHandler
import com.berryworks.edireader.error.RecoverableSyntaxException
import org.xml.sax.XMLReader
import org.xml.sax.InputSource
import javax.xml.transform.sax.SAXSource
import javax.xml.transform.Transformer
import javax.xml.transform.TransformerConfigurationException
import javax.xml.transform.TransformerException
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.StreamResult
import java.io._

import scala.collection.mutable.ArrayBuffer



case class SelectRow(rowName:Row)
class EdiReader()
{
  def ediParser(): Any = {
    val inputReader: Reader = new InputStreamReader(new FileInputStream("/home/vishaka/Downloads/edireader/sample-data/test_claim_1.txt"), "ISO-8859-1")
//    val writer: Writer = new OutputStreamWriter(new FileOutputStream("/home/vishaka/Downloads/edireader/OP.xml"),"ISO-8859-1")
    val writer: StringWriter = new StringWriter()
    val inputSource = new InputSource(inputReader)
    val ediReader: XMLReader = new EDIReader()
    val source: SAXSource = new SAXSource(ediReader, inputSource)
    val transformer: Transformer = TransformerFactory.newInstance().newTransformer()
    val result: StreamResult = new StreamResult(writer)
    transformer.transform(source, result)
    inputReader.close()
    writer.toString
  }
//  def xmlParser(rDD: RDD[Row],sc):Any =
//  {
//
//  }
}


object Parser837 extends App {

  val conf = new SparkConf().setMaster("local").setAppName("EDIReader")
  val sc = new SparkContext(conf)
  val edi = new EdiReader()
  val outputXml = edi.ediParser()

//  val sqlContext = new SQLContext(sc)
//  import sqlContext.implicits._
//  var df = sqlContext.read
//    .format("com.databricks.spark.xml")
//    .option("rowTag","interchange")
//    .load("/home/vishaka/Downloads/edireader/OP.xml")
//
//  df.printSchema()

  val rdd = sc.parallelize(List(outputXml))
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  var df = new XmlReader().xmlRdd(sqlContext, rdd.map(x => x.toString))
  df.printSchema()
  var buffer = ArrayBuffer.empty[Long]
  df = df.select($"interchange.group.transaction")
//  df = df.select($"group.transaction")

  var explodeDf = sqlContext.emptyDataFrame
  var i =0
  while(hasColumn(df, "loop")) {
    val columnName = df.columns{0}
    df = df.selectExpr(s"explode($columnName.loop) as loop")
    if (explodeDf.rdd.isEmpty()) {
      explodeDf = df.selectExpr("explode(loop.segment) as Segment", "loop")
        .selectExpr("explode(Segment.element) as ele", "loop")
        .select("loop.@Id", "ele.@Id", "ele.#VALUE")
        .toDF("LoopId", "ElementId", "Value")
//      val Code = when('Value, "41")
      val Code = when(col("ElementId").equalTo("NM101") || col("ElementId").equalTo("HL01"), col("Value"))
      explodeDf = explodeDf.withColumn("Code", Code)
    } else{
      var tempDf = df.selectExpr("explode(loop.segment) as Segment", "loop")
        .selectExpr("explode(Segment.element) as ele", "loop")
        .select("loop.@Id", "ele.@Id", "ele.#VALUE")
        .toDF("LoopId", "ElementId", "Value")
      val Code = when(col("ElementId").equalTo("NM101") || col("ElementId").equalTo("HL01"), col("Value"))
      tempDf = tempDf.withColumn("Code", Code)
      explodeDf = explodeDf.unionAll(tempDf)
    }

//    explodeDf.show()

//    before uncommenting below statements add 'libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"' to build.sbt
//    explodeDf.coalesce(1)       //use coalesce only with small data when you use csv
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("/home/vishaka/Desktop/output"+i+".csv")
//
//    i+=1

  }

  explodeDf.registerTempTable("claims")

  val loop1000 = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 1000")
  loop1000.show(40)

  loop1000.foreach { column =>
    (column.getLong(0), column.getString(1), column.getString(2)) match {
      case (1000, "NM101", "41") => submitter(loop1000)
      case (1000, "NM101", "40") => receiver(loop1000)
      case _ => true
    }
  }

  val loop2010 = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2010")
  loop2010.show(40)

  loop2010.foreach { column =>
    (column.getLong(0), column.getString(1), column.getString(2)) match {
      case (2010, "NM101", "IL") => subscriber(loop2010)
      case (2010, "NM101", "PR") => payer(loop2010)
      case (2010, "NM101", "85") => billingProvider(loop2010)
      case _ => true
    }
  }

  def hasColumn(df: DataFrame, columnName: String): Boolean = {
    val column = df.columns{0}
    df.select(s"$column.*").columns.contains(s"$columnName")
  }

  def submitter(df: DataFrame): Any = {
//    val submitterName = df.map{ row =>
//      val lastName = if (row.getString(1) == "NM103") row.getString(2) else ""
//      val firstName = if (row.getString(1) == "NM104") row.getString(2) else ""
//      val middleName = if (row.getString(1) == "NM105") row.getString(2) else ""
//      lastName + firstName + middleName
//    }
//    submitterName.collect().foreach(println)
  }
  def receiver(df: DataFrame): Any = {

  }

  def subscriber(df: DataFrame): Any = {

  }

  def payer(df: DataFrame): Any = {

  }

  def billingProvider(df: DataFrame): Any = {

  }

}
