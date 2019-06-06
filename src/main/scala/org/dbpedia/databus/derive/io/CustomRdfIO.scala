package org.dbpedia.databus.derive.io

import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.common.io.riot.lang.LangNTriplesSkipBad
import net.sansa_stack.rdf.common.io.riot.tokens.TokenizerTextForgiving
import net.sansa_stack.rdf.spark.io.RDFWriter
import org.aksw.commons.collections.cache.CountingIterator
import org.apache.commons.io.FileUtils
import org.apache.jena.atlas.io.PeekReader
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RIOT
import org.apache.jena.riot.system._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.derive.io.CustomRdfIO.CheckedTriple
import java.io.{File, FileOutputStream, FileWriter}

import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

case class BufferedLine(pos: Long, line: String)

object CustomRdfIO {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FLAT Triple Parser")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val bigger = spark.sparkContext.textFile(args(0))

    FileUtils.deleteDirectory(new File("test.nt"))
    FileUtils.deleteDirectory(new File("test.report"))

    val parsed = parse(bigger)

    val outTrFile = new File("out.nt")
    val outReFile = new File("out.report")

    outTrFile.delete()
    outReFile.delete()

    val outTriple = new FileWriter(outTrFile)
    val outReport = new FileWriter(outReFile)

    parsed.toLocalIterator.foreach( {
      case CheckedTriple(triple,None) => outTriple.write(s"${triple.get}\n"); outTriple.flush()
      case CheckedTriple(None,report) => outReport.write(s"${report.get}\n"); outReport.flush()
    })

//      parsed.filter(_.ntLine.isDefined).map(_.ntLine.get).saveAsNTriplesFile("test.nt")
//      parsed.filter(_.report.isDefined).map(_.report.get).saveAsTextFile("test.report")
  }

  case class CheckedTriple(ntLine: Option[Triple], report: Option[String] ) {
    override def toString: String = s"${ntLine.getOrElse("")}${report.getOrElse("")}}"
  }

  def parse(dataset: RDD[String]): RDD[CheckedTriple] = {

    dataset.mapPartitions(iterTriple => {

      val errorHandler = new NtErrorHandler

      val parserProfile = NonSerializableObjectWrapper {
        new ParserProfileStd(RiotLib.factoryRDF, errorHandler,
          IRIResolver.create, PrefixMapFactory.createForInput,
          RIOT.getContext.copy, true, true)
      }

      val iterCntTriple = new LineBufferIterator(new CountingIterator[String](iterTriple.asJava))
      val strmTriple = ReadableByteChannelFromIterator.toInputStream(iterCntTriple.asJava)

      val tokenizer = new TokenizerTextForgiving(PeekReader.makeUTF8(strmTriple))
      tokenizer.setErrorHandler(errorHandler)

      val it = new LangNTriplesSkipBad(tokenizer, parserProfile.get, null)

      new CombineIterator(it.asScala, errorHandler, iterCntTriple)
    })
  }
}

class CombineIterator(it: Iterator[Triple],
                      errorHandler: NtErrorHandler, iterLineBuffer: LineBufferIterator) extends Iterator[CheckedTriple]{

  var bufferedLine = BufferedLine(0,null)
  var changed = true

  override def hasNext: Boolean = { !errorHandler.hasNoErrors || it.hasNext }

  override def next(): CheckedTriple = {
    if( errorHandler.hasNoErrors ) {
      val next = it.next()
      if( iterLineBuffer.hasBufferedLine ) bufferedLine = iterLineBuffer.nextBufferedLine
      CheckedTriple(Some(next),None)
    } else {
      val bufferedError = errorHandler.dequeueError
      while ( bufferedLine.pos != bufferedError.pos ) { bufferedLine = iterLineBuffer.nextBufferedLine }
      CheckedTriple(None,Some(s"${bufferedLine.line} | ${bufferedError.line}"))
    }
  }
}

class LineBufferIterator(it: CountingIterator[String]) extends Iterator[String] {

  var lineBuffer = new mutable.Queue[BufferedLine]

  def getNumItems: Long = {
    it.getNumItems
  }

  def nextBufferedLine: BufferedLine  = {
    lineBuffer.dequeue
  }

  def hasBufferedLine: Boolean = {
    lineBuffer.isEmpty
  }

  override def hasNext: Boolean = {
    it.hasNext
  }

  override def next(): String = {
    val next = it.next
    lineBuffer += BufferedLine(it.getNumItems,next)
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
