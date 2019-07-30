package org.dbpedia.databus.derive.io.rdf

import java.io._
import java.math.BigInteger
import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest

import cats.effect.{ContextShift, IO}
import fs2.{Pure, Stream, io, text}
import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.common.io.riot.lang.LangNTriplesSkipBad
import net.sansa_stack.rdf.common.io.riot.tokens.TokenizerTextForgiving
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.jena.atlas.io.PeekReader
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.system._
import org.apache.jena.riot.{RDFDataMgr, RIOT}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.matching.Regex

object FlatRDFTripleParser {

  protected val ByteInputBufferSize: Int = 32 * 1024

  def main(args: Array[String]): Unit = {

    val proc = Runtime.getRuntime.availableProcessors()
    val par = args.par

    par.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(Math.ceil(proc/3.0).toInt))

    par.foreach(arg => {
      println(arg)
      val cis = {
        new CompressorStreamFactory()
          .createCompressorInputStream(
            new BufferedInputStream(
              new FileInputStream(
                new File(args(0)))))
      }

      val tripleOutput: OutputStream = new FileOutputStream(new File(s"$arg.out"))
      val reportOutput: OutputStream = new FileOutputStream(new File(s"$arg.err"))
      FlatRDFTripleParser.parse(cis,tripleOutput,reportOutput)
    })
  }

  def parse(
             tripleInput: InputStream, tripleOutput: OutputStream, reportOutput: OutputStream,
             par: Int = 3, chunk: Int = 200, reportFromat: ReportFormat.Value = ReportFormat.TEXT
           ): Unit = {


    implicit val executionContext: ExecutionContext =
      scala.concurrent.ExecutionContext.Implicits.global

    implicit val contextShift: ContextShift[IO] =
      IO.contextShift(executionContext)

    io.readInputStream[IO](IO(tripleInput),ByteInputBufferSize,executionContext)
      .through(text.utf8Decode)
      .through(text.lines)
      .chunkN(chunk)
      .parEvalMap(par)( x => IO {

        val reports = reportFromat match {
          case ReportFormat.TEXT =>
            new BufferedTextReportsEH(x.toArray)
          case ReportFormat.RDF =>
            new BufferedRDFReportsEH(x.toArray)
        }

        val parserProfile = {
          new ParserProfileStd(RiotLib.factoryRDF, reports,
            IRIResolver.create, PrefixMapFactory.createForInput,
            RIOT.getContext.copy, true, true)
        }

        val strmTriple = ReadableByteChannelFromIterator.toInputStream(x.iterator.asJava)

        val tokenizer = new TokenizerTextForgiving(PeekReader.makeUTF8(strmTriple))
        tokenizer.setErrorHandler(reports)

        val jenaTriples = new LangNTriplesSkipBad(tokenizer, parserProfile, null)

        val tripleOS = new ByteArrayOutputStream()
        RDFDataMgr.writeTriples(tripleOS,jenaTriples)

        val reportStream = {

          if(reports.getReports.nonEmpty) {

            reports match {

              case textReports: BufferedTextReportsEH =>
                Stream(ReportBytes(textReports.getReports.mkString("", "\n", "\n").getBytes(UTF_8)))

              case rdfReports: BufferedRDFReportsEH =>
                val reportOS = new ByteArrayOutputStream()
                RDFDataMgr.writeTriples(reportOS,rdfReports.getReports.toIterator.asJava)
                Stream(ReportBytes(reportOS.toByteArray))
            }
          } else Stream.empty
        }

        Stream[Pure,CPR](TripleBytes(tripleOS.toByteArray))++ reportStream

      }).flatten.parEvalMap(1){

        case TripleBytes(bytes) => IO { tripleOutput.write(bytes); Stream.empty }
        case ReportBytes(bytes) => IO { reportOutput.write(bytes); Stream.empty }
      }
      .compile.drain.unsafeRunSync()
  }
}

sealed trait CPR
case class TripleBytes(bytes: Array[Byte]) extends CPR
case class ReportBytes(bytes: Array[Byte]) extends CPR

object ReportFormat extends Enumeration {
  val TEXT,RDF = Value
}

trait BufferedErrorHandler[T] {

  protected val reports: ListBuffer[T] = ListBuffer[T]()

  def getReports: ListBuffer[T] = {
    reports
  }
}

class BufferedTextReportsEH(rawLines: Array[String]) extends BufferedErrorHandler[String] with ErrorHandler{

  override def warning(message: String, line: Long, col: Long): Unit = {
    reports.append(s"${rawLines(line.toInt-1)} # WRN@$col $message")
  }

  override def error(message: String, line: Long, col: Long): Unit = {
    reports.append(s"${rawLines(line.toInt-1)} # ERR@$col $message")
  }

  override def fatal(message: String, line: Long, col: Long): Unit = {
    reports.append(s"${rawLines(line.toInt-1)} # FTL@$col $message")
  }
}

class BufferedRDFReportsEH(rawLines: Array[String]) extends BufferedErrorHandler[Triple] with ErrorHandler{

  def base: String = "http://dbpedia.org/debug/"
  def rIri: Regex = """<.*>""".r
  def rLit: Regex = """\".*\"""".r

  override def warning(message: String, line: Long, col: Long): Unit = {
    construct("e",message,line,col)
  }

  override def error(message: String, line: Long, col: Long): Unit = {
    construct("e",message,line,col)
  }

  override def fatal(message: String, line: Long, col: Long): Unit = {
    construct("e",message,line,col)
  }

  def construct(level: String, message: String, line: Long, col: Long): Unit = {

    val lIdx = line.toInt-1
    val resource = NodeFactory.createURI(s"${base}entity/${sha256FromString(rawLines(lIdx))}")

    appendReportBuffer(
      resource,NodeFactory.createURI(s"${base}vocab/raw"),
      NodeFactory.createLiteral(rawLines(lIdx)))

    appendReportBuffer(
      resource,NodeFactory.createURI(s"${base}vocab/pos"),
      NodeFactory.createLiteral(col.toString,XSDDatatype.XSDnonNegativeInteger))

    val m = rLit.replaceAllIn(rIri.replaceAllIn(message.split(" ",2)(1),""),"")

    appendReportBuffer(
      resource,NodeFactory.createURI(s"${base}vocab/code"),
      NodeFactory.createURI(s"${base}code/${URLEncoder.encode(m,"UTF-8")}"))
  }

  def appendReportBuffer(s: Node,p: Node,o: Node): Unit = {
    reports.append(new org.apache.jena.graph.Triple(s,p,o))
  }

  def sha256FromString(string: String): String = {
    String.format(
      "%032x", new BigInteger(
        1, MessageDigest.getInstance("SHA-256").
          digest(string.getBytes("UTF-8"))))
  }
}
