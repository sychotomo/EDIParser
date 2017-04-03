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
  def ediParser():String = {
    val inputReader: Reader = new InputStreamReader(new FileInputStream("/home/abhishek/Desktop/test1.txt"), "ISO-8859-1")
//    val generatedOutput:Writer = new OutputStreamWriter(new FileOutputStream
//      ("/home/abhishek/Desktop/output.xml"),"ISO-8859-1")
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
  val rdd = sc.parallelize(List(outputXml))
  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._
  var df = new XmlReader().xmlRdd(sqlContext,rdd)
  var buffer = ArrayBuffer.empty[Long]
  df = df.select($"interchange.group.transaction")
  df.selectExpr("explode(transaction.loop) as e").selectExpr("explode(e.segment) as Segment","e.Id as EId").selectExpr("explode(Segment.element) as ele","EId","Segment.Id as SId").show()//.selectExpr("explode(ele) as new_ele","EId","SId").show()
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
//    .load("/home/abhishek/Desktop/output.xml")
  //val df = ediRoot.select(explode($"interchange").as("ic"))
  //var explodeDF = ediRoot.withColumn("ic", ediRoot("interchange"))
  //ediRoot.printSchema()
//  val df = ediRoot.select($"group.transaction.segment.element")
//  df.select(explode($"element").as("new_ele")).collect.map(t => println(t))
}
