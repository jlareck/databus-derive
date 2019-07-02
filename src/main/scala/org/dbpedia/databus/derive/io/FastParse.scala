package org.dbpedia.databus.derive.io

import java.io._
import java.nio.charset.StandardCharsets.UTF_8

import cats.effect.{ContextShift, IO}
import fs2.{Pure, Stream, io, text}
import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
import net.sansa_stack.rdf.common.io.riot.lang.LangNTriplesSkipBad
import net.sansa_stack.rdf.common.io.riot.tokens.TokenizerTextForgiving
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.jena.atlas.io.PeekReader
import org.apache.jena.riot.system._
import org.apache.jena.riot.{RDFDataMgr, RIOT}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.ExecutionContext
import scala.concurrent.forkjoin.ForkJoinPool

sealed trait CPR
case class TripleBytes(bytes: Array[Byte]) extends CPR
case class ReportBytes(bytes: Array[Byte]) extends CPR

object FastParse {

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

      //      val tripleOutput: OutputStream = new PrintStream(System.out)
      val tripleOutput: OutputStream = new FileOutputStream(new File(s"$arg.out"))
      //      val reportOutput: OutputStream = new PrintStream(System.err)
      val reportOutput: OutputStream = new FileOutputStream(new File(s"$arg.err"))
      FastParse.parse(cis,tripleOutput,reportOutput)
    })
  }

  def parse(
             tripleInput: InputStream, tripleOutput: OutputStream, reportOutput: OutputStream,
             par: Int = 3, chunk: Int = 200
           ): Unit = {


    implicit val executionContext: ExecutionContext =
      scala.concurrent.ExecutionContext.Implicits.global

    implicit val contextShift: ContextShift[IO] =
      IO.contextShift(executionContext)

    io.readInputStream[IO](IO(tripleInput),ByteInputBufferSize,executionContext)
      .through(text.utf8Decode)
      .through(text.lines)
      .chunkN(100)
      .parEvalMap(par)( x => IO {

        val reports = new BufferingErrorHandler(x.toArray)

        val parserProfile = {
          new ParserProfileStd(RiotLib.factoryRDF, reports,
            IRIResolver.create, PrefixMapFactory.createForInput,
            RIOT.getContext.copy, true, true)
        }

        val strmTriple = ReadableByteChannelFromIterator.toInputStream(x.iterator.asJava)

        val tokenizer = new TokenizerTextForgiving(PeekReader.makeUTF8(strmTriple))
        tokenizer.setErrorHandler(reports)

        val jenaTriples = new LangNTriplesSkipBad(tokenizer, parserProfile, null)

        val triples = new ByteArrayOutputStream()
        RDFDataMgr.writeTriples(triples,jenaTriples)

        val reportBuffer = reports.getReports

        val reportStream = if(reportBuffer.nonEmpty) {
          Stream(ReportBytes(reportBuffer.mkString("", "\n", "\n").getBytes(UTF_8)))
        } else Stream.empty

        Stream[Pure,CPR](TripleBytes(triples.toByteArray))++ reportStream

      }).flatten.parEvalMap(1){

        case TripleBytes(bytes) => IO { tripleOutput.write(bytes); Stream.empty }
        case ReportBytes(bytes) => IO { reportOutput.write(bytes); Stream.empty }
      }
      .compile.drain.unsafeRunSync()
  }
}

class BufferingErrorHandler(rawLines: Array[String]) extends ErrorHandler{

  private val reports = ListBuffer[String]()

  def getReports: ListBuffer[String] = {
    reports
  }

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
