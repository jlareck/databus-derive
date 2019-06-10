package org.dbpedia.databus.derive.io

import java.io.File

import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.common.io.riot.lang.LangNTriplesSkipBad
import net.sansa_stack.rdf.common.io.riot.tokens.TokenizerTextForgiving
import org.apache.commons.io.FileUtils
import org.apache.jena.atlas.io.PeekReader
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.system._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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

    val spark = SparkSession.builder()
      .appName("FLAT Triple Parser")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max","512m")
      .getOrCreate()

    val bigger = spark.sparkContext.textFile(args(0),Runtime.getRuntime.availableProcessors()*3)

    FileUtils.deleteDirectory(new File("test.nt"))
    FileUtils.deleteDirectory(new File("test.report"))

    val parsed = parse(bigger).persist(StorageLevel.MEMORY_AND_DISK)

    writeTripleReports(parsed, Some(new File("test.nt")), Some(new File("test.report")))
  }


  def parse(dataset: RDD[String]): RDD[TripleReport] = {

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
  def writeTripleReports(tripleReports: RDD[TripleReport],
                         tripleSink: Option[File], reportSink: Option[File]): Unit = {

    if( tripleSink.isEmpty && reportSink.isEmpty) {
      // TODO to triple to system.out and reports to system.err
    } else {

      if(tripleSink.isDefined) {
        tripleReports.filter(_.triple.isDefined).map(_.triple.get)
          .saveAsNTriplesFile(tripleSink.get.getAbsolutePath)
      }

      if(reportSink.isDefined) {
        tripleReports.filter(_.report.isDefined).map(_.report.get)
          .saveAsTextFile(reportSink.get.getAbsolutePath,compression)
      }
    }
  }
}

class CombineQueuesIterator[T](triples: Iterator[Triple],
reports: QueuedErrorHandler, lines: QueuedIterator[String]) extends Iterator[TripleReport]{

  var row = RowObject(0,"")
  var changed = true

  override def hasNext: Boolean = { !reports.isQueueEmpty || triples.hasNext }

  override def next(): TripleReport = {

    if( reports.isQueueEmpty ) {
      val next = triples.next
      if( ! lines.isQueueEmpty ) row = lines.dequeue
      TripleReport(Some(next),None)
    } else {
      val report = reports.nextReport
      while ( row.pos < report.pos ) { row = lines.dequeue }
      TripleReport(None,Some(s"${row.line} # ${report.line}"))
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
