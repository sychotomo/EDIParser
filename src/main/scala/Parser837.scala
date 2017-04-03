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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

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
  df.select("interchange.@Date").show()
  df = df.select($"interchange.group.transaction")
//  df = df.select($"group.transaction")
  val loop1000_2000 = df.selectExpr("explode(transaction.loop) as l")
    .selectExpr("explode(l.segment) as Segment", "l")
    .selectExpr("explode(Segment.element) as ele", "l")
    .select("l.@Id", "ele.@Id", "ele.#VALUE")
    .toDF("LoopId", "ElementId", "Value")
  loop1000_2000.show()


  val loop2010 = df.selectExpr("explode(transaction.loop) as l1")
    .selectExpr("explode(l1.loop) as l")
    .selectExpr("explode(l.segment) as Segment", "l")
    .selectExpr("explode(Segment.element) as ele", "l")
    .select("l.@Id", "ele.@Id", "ele.#VALUE")
    .toDF("LoopId", "ElementId", "Value")
  loop2010.show()


  val subscriberName = loop2010.filter(loop2010("Value").equalTo("IL"))
    .select("ElementId")
    .show()

  val loop2310 = df.selectExpr("explode(transaction.loop) as l1")
    .selectExpr("explode(l1.loop) as l2")
    .selectExpr("explode(l2.loop) as l")
    .selectExpr("explode(l.segment) as Segment", "l")
    .selectExpr("explode(Segment.element) as ele", "l")
    .select("l.@Id", "ele.@Id", "ele.#VALUE")
    .toDF("LoopId", "ElementId", "Value")
  loop2310.show()


  val loop2330 = df.selectExpr("explode(transaction.loop) as l1")
    .selectExpr("explode(l1.loop) as l2")
    .selectExpr("explode(l2.loop) as l3")
    .selectExpr("explode(l3.loop) as l")
    .selectExpr("explode(l.segment) as Segment", "l")
    .selectExpr("explode(Segment.element) as ele", "l")
    .select("l.@Id", "ele.@Id", "ele.#VALUE")
    .toDF("LoopId", "ElementId", "Value")
  loop2330.show()





  //df.select(explode($"loop").as("new_loop")).select(explode($"new_loop.segment.element").as("new_ele")).foreach(t=>println(t(0)))
//    .collect().map(t=>{
//    println(t(0).getClass)
//  })
  //buffer.foreach(t=>println(t(0)))
  //sqlContext.sql(""" select * from edi """).show()
  //val rdd = sc.parallelize(outputXml)
//  val sqlContext = new SQLContext(sc)
//  import sqlContext.implicits._
//  val ediRoot = sqlContext.read
//    .format("com.databricks.spark.xml")
//      .option("rowTag","interchange")
//    .load("/home/vishaka/Downloads/edireader/OP.xml")
//  val df = ediRoot.select(explode($"interchange").as("ic"))
//  var explodeDF = ediRoot.withColumn("ic", ediRoot("interchange"))
//  ediRoot.printSchema()
//  val df = ediRoot.select($"group.transaction.segment.element")
//  df.select(explode($"element").as("new_ele")).collect.map(t => println(t))
}
