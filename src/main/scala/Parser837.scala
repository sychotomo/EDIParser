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

import org.apache.spark.sql.types._

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
  var subElementDf = sqlContext.emptyDataFrame
  var columnName = df.columns{0}

  while(hasColumn(df, columnName, "loop")) {
    df = df.selectExpr(s"explode($columnName.loop) as loop")
    columnName = df.columns{0}

    if (explodeDf.rdd.isEmpty()) {
      explodeDf = df.selectExpr("explode(loop.segment) as Segment", "loop")
        .selectExpr("explode(Segment.element) as ele", "loop")
        .select("loop.@Id", "ele.@Id", "ele.#VALUE")
        .toDF("LoopId", "ElementId", "EleValue")
      val Code = when(col("ElementId").equalTo("NM101") || col("ElementId").equalTo("HL01"), col("EleValue"))
      explodeDf = explodeDf.withColumn("Code", Code)

//      if (explodeDf.col("EleValue") == null) {
//        explodeDf = explodeDf.selectExpr("explode(ele.subelement) as subele", "ele", "LoopId")
//          .select("subele.@Sequence", "subele.#VALUE", "ele.#VALUE", "ele", "loop")
//          .toDF("Sequence", "SubValue", "EleValue", "ele", "loop")
//      }
//      val Sequence = when(col("EleValue").equalTo(null), col("Sequence")).otherwise(null)
//      val Value = when(col("EleValue").equalTo(null), col("SubValue")).otherwise(col("EleValue"))
//      explodeDf = explodeDf.withColumn("Value", Value)
//        .withColumn("Sequence", Sequence)
//        .select("loop.@Id", "ele.@Id", "Value", "Sequence")
//        .toDF("LoopId", "ElementId", "Value", "Sequence")

//      explodeDf.show()
    } else{
      var tempDf = df.selectExpr("explode(loop.segment) as Segment", "loop")
        .selectExpr("explode(Segment.element) as ele", "loop")
        .select("loop.@Id", "ele.@Id", "ele.#VALUE", "ele")
        .toDF("LoopId", "ElementId", "EleValue", "ele")

      val Code = when(col("ElementId").equalTo("NM101") || col("ElementId").equalTo("HL01") || col("ElementId").equalTo("CLM01"), col("EleValue"))
      tempDf = tempDf.withColumn("Code", Code)

      if (hasColumn(tempDf, "ele", "subelement")) {
        if (subElementDf.rdd.isEmpty()) {
          subElementDf = tempDf.selectExpr("explode(ele.subelement) as subele", "LoopId", "ElementId", "Code")
            .select("LoopId", "ElementId", "subele.@Sequence", "subele.#VALUE", "Code")
            .toDF("LoopId", "ElementId", "Sequence", "Value", "Code")
        } else{
          val subElementTempDf = tempDf.selectExpr("explode(ele.subelement) as subele", "LoopId", "ElementId", "Code")
            .select("LoopId", "ElementId", "subele.@Sequence", "subele.#VALUE", "Code")
            .toDF("LoopId", "ElementId", "Sequence", "Value", "Code")

          subElementDf = subElementDf.unionAll(subElementTempDf)
        }
      }
//      tempDf.foreach { col =>
//        col.getAs[String]("EleValue") match {
//          case null => val subelementDf = tempDf.selectExpr(s"explode(${col.getAs("ele")}.subelement) as subele", "ele", "loop")
//            .select("subele.@Sequence", "subele.#VALUE", "ele.#VALUE", "ele.@Id")
//            .toDF("Sequence", "SubValue", "EleValue", "EleId")
//            println("##")
//            tempDf.printSchema()
//          case _ =>
//        }
//      }


      tempDf = tempDf.select("LoopId", "ElementId", "EleValue", "Code")
      explodeDf = explodeDf.unionAll(tempDf)
    }

//    explodeDf.show()
//    explodeDf.printSchema()

    subElementDf.show()


//    before uncommenting below statements add 'libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.5.0"' to build.sbt
//    explodeDf.coalesce(1)       //use coalesce only with small data when you use csv
//      .write
//      .mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("/home/vishaka/Desktop/output")
  }
  var token = ""
  val explodeRdd = explodeDf.map( row => {
    if (row.getAs[String]("Code") != null) {
      token = row.getAs[String]("Code")
    }
    Row(row(0), row(1), row(2), token)
  })

  val schema =   StructType(
    StructField("LoopId", LongType, true) ::
      StructField("ElementId", StringType, true) ::
      StructField("Value", StringType, true) ::
      StructField("Code", StringType, true) :: Nil)

  df = sqlContext.createDataFrame(explodeRdd, schema)
  df.show(732)
//
//
//  df.registerTempTable("claims")
//
//  val loopSubmitter = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 1000 and Code = '41'")
//  loopSubmitter.show()
//  submitter(loopSubmitter)
//
//  val loopReceiver = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 1000 and Code = '40'")
//  loopReceiver.show()
//  receiver(loopReceiver)
//
//  val loopSubscriber = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2010 and Code = 'IL'")
//  loopSubscriber.show()
//  subscriber(loopSubscriber)
//
//  val loopPayer = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2010 and Code = 'PR'")
//  loopPayer.show()
//  payer(loopPayer)
//
//  val loopBillingProvider = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2010 and Code = '85'")
//  loopBillingProvider.show()
//  billingProvider(loopBillingProvider)
//
//  val loopPatient = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2010 and Code = 'QC'")
//  loopPatient.show()
////  patient(loopPatient)
//
//  val loopClaim = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2300")
//  loopClaim.show()
//
//  val loopReferringProvider = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2310 and Code = 'DN'")
//  loopReferringProvider.show()
//
//  val loopRenderingProvider = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2310 and Code = '82'")
//  loopRenderingProvider.show()
//
//  val loopServiceFacility = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2310 and Code = '77'")
//  loopServiceFacility.show()
//
//  val loopSupervisingProvider = sqlContext.sql("SELECT LoopId, ElementId, Value, Code FROM claims WHERE LoopId = 2310 and Code = 'DQ'")
//  loopSupervisingProvider.show()

  //  loop2010.foreach { column =>
//    (column.getLong(0), column.getString(1), column.getString(2)) match {
//      case (2010, "NM101", "IL") => subscriber(loop2010)
//      case (2010, "NM101", "PR") => payer(loop2010)
//      case (2010, "NM101", "85") => billingProvider(loop2010)
//      case _ => true
//    }
//  }

  def hasColumn(df: DataFrame, column: String, subColumn: String): Boolean = {
//    val column = df.columns{0}
    df.select(s"$column.*").columns.contains(s"$subColumn")
  }
}
