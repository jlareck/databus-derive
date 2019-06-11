package org.dbpedia.databus.derive.io

import java.io.{ByteArrayOutputStream, File, FileWriter, PrintWriter}
import java.util.Collections

import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.common.io.riot.lang.LangNTriplesSkipBad
import net.sansa_stack.rdf.common.io.riot.tokens.TokenizerTextForgiving
import net.sansa_stack.rdf.spark.io.RDFLang
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.compress.CompressionOutputStream
import org.apache.jena.atlas.io.PeekReader
import org.apache.jena.graph.Triple
import org.apache.jena.riot.{Lang, RDFDataMgr, RIOT}
import org.apache.jena.riot.system._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

/**
  * @author Marvin Hofer
  *         Custom line based triple parser including generation of parse logs.
  *         Parse logs are of the type [INVALID_TRIPLE # REPORT].
  */
object CustomRdfIO {

  /**
    * Example code.
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {

    val tmpSpark = s"tmp.${java.util.UUID.randomUUID.toString}"

    val spark = SparkSession.builder()
      .appName("FLAT Triple Parser")
      .master("local[*]")
      .config("spark.local.dir",tmpSpark)
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val sql = spark.sqlContext

    val bigger = sql.read.textFile(args(0)).repartition(Runtime.getRuntime.availableProcessors()*3)

    FileUtils.deleteDirectory(new File("test.nt"))
    FileUtils.deleteDirectory(new File("test.report"))

    val parsed = parse(bigger)(sql)//.persist(StorageLevel.DISK_ONLY)

    writeTripleReports(parsed, Some(new File("test.nt")), Some(new File("test.report")))(sql)
  }


  def parse(dataset: Dataset[String])(implicit SQLContext: SQLContext): Dataset[TripleReportString] = {


    import SQLContext.implicits._

    dataset.mapPartitions(iterTriple => {

      val errorHandler = new QueuedErrorHandler

      val parserProfile = NonSerializableObjectWrapper {
        new ParserProfileStd(RiotLib.factoryRDF, errorHandler,
        IRIResolver.create, PrefixMapFactory.createForInput,
        RIOT.getContext.copy, true, true)
      }

      val iterCntTriple = new QueuedIterator[String](iterTriple)
      val strmTriple = ReadableByteChannelFromIterator.toInputStream(iterCntTriple.asJava)

      val tokenizer = new TokenizerTextForgiving(PeekReader.makeUTF8(strmTriple))
      tokenizer.setErrorHandler(errorHandler)

      val baos = new ByteArrayOutputStream()
//      val writer = StreamRDFWriter.getWriterStream(baos, Lang.NTRIPLES)

      val it = new LangNTriplesSkipBad(tokenizer, parserProfile.get, null)



      new CombineQueuesIterator(it.asScala, errorHandler, iterCntTriple)
    })
  }

  /**
    * Write TripleReports to File or System.out/err
    * @param tripleReports RDD[TripleReport] containing triple report lines
    * @param tripleSink Sink to write valid triple
    * @param reportSink Sink to write invalid triple and report
    */
  def writeTripleReports(tripleReports: Dataset[TripleReportString],
                         tripleSink: Option[File], reportSink: Option[File])(implicit SQLContext: SQLContext): Unit = {

    import SQLContext.implicits._

//    val streaming = tripleReports.filter(_.triple.isDefined).map(_.triple.get).toDF().writeStream
//      .trigger(Trigger.Once())
//      .outputMode(OutputMode.Append())
//      .format("text")
//      .option("path", tripleSink.get.getAbsolutePath)
//      .option("compression", "bzip2")
//      .start()
//    streaming.awaitTermination()

    if( tripleSink.isEmpty && reportSink.isEmpty) {
      // TODO to triple to system.out and reports to system.err
    } else {

      if(tripleSink.isDefined) {
        tripleReports.filter(_.triple.isDefined).map(_.triple.get)
          .write
          .option("compression", "bzip2")
          .text(tripleSink.get.getAbsolutePath)
      }

      if(reportSink.isDefined) {
        tripleReports.filter(_.report.isDefined).map(_.report.get)
          .write
          .option("compression", "bzip2")
          .text(reportSink.get.getAbsolutePath)
      }
    }
  }
}

class CombineQueuesIterator[T](triples: Iterator[Triple],
reports: QueuedErrorHandler, lines: QueuedIterator[String]) extends Iterator[TripleReportString]{

  val os = new ByteArrayOutputStream()
  RDFDataMgr.writeTriples(os, triples.asJava)
  val tripleLines = Collections.singleton(new String(os.toByteArray)).iterator().asScala

  var row = RowObject(0,"")
  var changed = true

  override def hasNext: Boolean = { !reports.isQueueEmpty || tripleLines.hasNext }

  override def next(): TripleReportString = {

    if( reports.isQueueEmpty ) {
      val next = tripleLines.next
      if( ! lines.isQueueEmpty ) row = lines.dequeue
      TripleReportString(Some(next),None)
    } else {
      val report = reports.nextReport
      while ( row.pos < report.pos ) { row = lines.dequeue }
      TripleReportString(None,Some(s"${row.line} # ${report.line}"))
    }
  }
}

class QueuedIterator[T](it: Iterator[T]) extends Iterator[T] {

  protected var currentPosition = 0L
  var rows = new mutable.Queue[RowObject[T]]

  def dequeue: RowObject[T]  = {
    rows.dequeue
  }

  def isQueueEmpty: Boolean = {
    rows.isEmpty
  }

  override def hasNext: Boolean = {
    it.hasNext
  }

  override def next(): T = {
    val next = it.next
    this.currentPosition += 1
    rows += RowObject(currentPosition,next)
    next
  }
}

private class NonSerializableObjectWrapper[T: ClassTag](constructor: => T) extends AnyRef with Serializable {
  @transient private lazy val instance: T = constructor

  def get: T = instance
}

private object NonSerializableObjectWrapper {
  def apply[T: ClassTag](constructor: => T): NonSerializableObjectWrapper[T] = new NonSerializableObjectWrapper[T](constructor)
}
