package org.dbpedia.databus.derive.io

import better.files._
import cats.effect.IO._
import cats.effect.internals.IOContextShift
import cats.effect.{ContextShift, ExitCode, IO, IOApp}
import cats.kernel.Semigroup
import cats.syntax.all._
import com.typesafe.scalalogging.LazyLogging
import fs2.{Chunk, Pipe, Pure, Stream, io, text}
import monix.execution.Scheduler
import monix.execution.atomic.Atomic
import org.apache.jena.graph
import org.apache.jena.riot.system.{ErrorHandler, StreamRDF, StreamRDFWriter}
import org.apache.jena.riot.{Lang => RDFLang, _}
import org.apache.jena.sparql.core.Quad

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Success

import java.io.{File => _, _}
import java.nio.charset.StandardCharsets.UTF_8

object RobustRDFLinesParser {

  protected lazy val parsingIoScheduler = Scheduler.cached(s"parsing-io", 2, Int.MaxValue)

  protected val ByteInputBufferSize = 32 * 1024
}

case class RobustRDFLinesParser(
  sourceFile: Option[File] = None,
  sinkFile: Option[File] = None,
  errorsFile: Option[File] = None,
  warningsFile: Option[File] = None,
  inFormat: RDFLang = RDFLang.NTRIPLES,
  outFormat: RDFLang = RDFLang.NTRIPLES,
  parsingsChecks: Boolean = true,
  warningsToErrors: Boolean = false,
  parallelism: Int = 1,
  linesChunkSize: Int = 100 ) extends LazyLogging {

  import RobustRDFLinesParser._

  def parseTo(file: File) = copy(sinkFile = Some(file)).doParse()

  def parseToStdOut() = copy(sinkFile = None).doParse()

  def doParse(): IO[Option[ParsingStats]] = {

    implicit val parsingReportCombiner = Semigroup.instance[ParsingStats] { (a, b) =>
      ParsingStats(a.triplesRead + b.triplesRead, a.warnings + b.warnings, a.errors + b.errors)
    }

    def processingResultsWriter(triplesSink: OutputStream, errorsSink: OutputStream,
      warningsSink: OutputStream): ChunkedProcessingResult => IO[Stream[Pure, ParsingStats]] = {

        case ReserializedTriples(bytes) => IO { triplesSink.write(bytes); Stream.empty }

        case WarningBytes(bytes) => IO { warningsSink.write(bytes); Stream.empty }

        case ErrorBytes(bytes) => IO { errorsSink.write(bytes); Stream.empty }

        case report: ParsingStats => IO { Stream(report) }
    }

    case class FlushInsteadClose(os: OutputStream) extends FilterOutputStream(os) {

      override def close(): Unit = os.flush()
    }

    def fileOrStdStreamSink(fileOption: Option[File])(stdFallback: PrintStream) = {

      fileOption.fold(FlushInsteadClose(stdFallback): OutputStream) {
        _.newOutputStream.buffered
      }
    }

    def tripleSink = fileOrStdStreamSink(sinkFile)(System.out)

    lazy val errorSink = fileOrStdStreamSink(errorsFile)(System.err)

    def warningSink = if(warningsToErrors) errorSink else fileOrStdStreamSink(warningsFile)(System.err)

    val streamsBracket = Stream.bracket(IO { (tripleSink, errorSink, warningSink) })({ case (t, e, w) =>

      implicit val contextShift = IOContextShift.global

      (IO(t.close()).attempt, IO(e.close()).attempt, IO(w.close()).attempt) parMapN { case (_,_,_) => () }
    })

    streamsBracket.flatMap({ case streams =>


      val chunkProcessing = parseInputStream
        .through(text.utf8Decode).through(text.lines)
        .zipWithIndex.chunkN(linesChunkSize)

      val toFiles: Pipe[IO, Stream[Pure, ChunkedProcessingResult], ParsingStats] = {

        val writer = (processingResultsWriter _) tupled streams

        implicit val contextShift = IOContextShift(parsingIoScheduler)

        _.flatten.parEvalMap(1)(writer).flatten
      }


      if(parallelism > 1) {

        implicit val contextShift = IOContextShift.global

        chunkProcessing.parEvalMap(parallelism)(processLineChunk).through(toFiles)
      } else {
        chunkProcessing.evalMap(processLineChunk).through(toFiles)
      }

    }).compile.foldSemigroup
  }

  protected def processLineChunk(lineChunk: Chunk[(String, Long)]): IO[Stream[Pure, ChunkedProcessingResult]] =
    IOContextShift.global.shift *> IO {

      val chunkStartIdx = lineChunk.head.get._2

      val lines = lineChunk.map(_._1).toArray

      val triplesBuffer = ArrayBuffer[Byte]()

      val warningsBuffer = ListBuffer[Warning]()

      val errorsBuffer = ListBuffer[ParseError]()

      val parsedTriplesQuads = Atomic(0L)

      def parse(startIdx: Int): Unit = {

        parseToBuffers(triplesBuffer, warningsBuffer, lines, startIdx, chunkStartIdx) match {

          case (None, parsed) => parsedTriplesQuads += parsed

          case (Some(error: ParseError), parsed) => {
            errorsBuffer append error
            parsedTriplesQuads += parsed

            // to get the correct shift for the start index, we have to convert the 'global' line index back to 'local'
            def parseRunErrorIndex = error.lineIndex - startIdx - chunkStartIdx

            parse(startIdx + parseRunErrorIndex.toInt)
          }
        }
      }

      logger.debug(s"processing chunk starting at line ${chunkStartIdx + 1}")
      parse(0)
      logger.debug(s"finished chunk starting at line ${chunkStartIdx + 1}")

      def warnings = if(warningsBuffer.nonEmpty) {
        Stream(WarningBytes(warningsBuffer.mkString("", "\n", "\n").getBytes(UTF_8)))
      } else Stream.empty

      def errors = if(errorsBuffer.nonEmpty) {
        Stream(ErrorBytes(errorsBuffer.mkString("", "\n", "\n").getBytes(UTF_8)))
      } else Stream.empty

      def report = ParsingStats(parsedTriplesQuads.get, warningsBuffer.size, errorsBuffer.size)

      Stream(ReserializedTriples(triplesBuffer.toArray), report) ++ warnings ++ errors
    }

  protected def stringIterToInputStream(stringIter: Iterator[String]): SequenceInputStream = {

    new SequenceInputStream({

      val inputStreamIter = stringIter map {
        s => new ByteArrayInputStream((s + "\n").getBytes(UTF_8))
      }

      inputStreamIter.asJavaEnumeration
    })
  }

  protected class CollectingErrorHandler(warningsBuffer: ListBuffer[Warning], errorFlag: Promise[Option[ParseError]],
    linesOfChunk: Array[String], attemptStartIdx: Int, chunkStartIdx: Long) extends ErrorHandler {

    def chunkLineIdx(i: Long) = attemptStartIdx + i.toInt - 1

    def sourceLineIdx(i: Long) = chunkStartIdx + attemptStartIdx + i.toInt

    override def warning(message: String, line: Long, col: Long): Unit = {
      warningsBuffer append Warning(message, linesOfChunk(chunkLineIdx(line)), sourceLineIdx(line), col.toInt)
    }

    override def error(message: String, line: Long, col: Long): Unit = {
      errorFlag complete Success(Some(Error(message, linesOfChunk(chunkLineIdx(line)), sourceLineIdx(line), col.toInt)))
    }

    override def fatal(message: String, line: Long, col: Long): Unit = {
      errorFlag complete Success(Some(Fatal(message, linesOfChunk(chunkLineIdx(line)), sourceLineIdx(line), col.toInt)))
    }
  }

  protected def parseToBuffers(triplesBuffer: ArrayBuffer[Byte], warningsBuffer: ListBuffer[Warning],
    lines: Array[String], attemptStartIdx: Int, chunkStartIdx: Long) = {

    val parseErrorFlag = Promise[Option[ParseError]]

    def inputStream = stringIterToInputStream(lines.iterator.slice(attemptStartIdx, lines.size))

    val parser = RDFParser.create().source(inputStream)
      .forceLang(inFormat)
      .checking(parsingsChecks)
      .errorHandler(new CollectingErrorHandler(warningsBuffer, parseErrorFlag, lines, attemptStartIdx, chunkStartIdx))

    val baos = new ByteArrayOutputStream()

    val writer = new CountingStreamRDF(StreamRDFWriter.getWriterStream(baos, outFormat))

    val parsingProcess = Future {
      parser.parse(writer)
      parseErrorFlag.complete(Success(None))
    }

    val join = parsingProcess.transformWith(_ => parseErrorFlag.future)

    val errorFlag = Await.result(join, 10 minutes)

    triplesBuffer ++= baos.toByteArray

    (errorFlag, writer.tripleQuadCount.get)
  }

  protected def parseInputStream = {

    implicit val contextShift: ContextShift[IO] = IO.contextShift(parsingIoScheduler)

    sourceFile.fold(io.stdin[IO](ByteInputBufferSize, parsingIoScheduler)) {

      file => io.file.readAll[IO](file.path, parsingIoScheduler, ByteInputBufferSize)
    }
  }

  def sourceFromFile(file:File) =  copy(sourceFile = Some(file))

  def sinkIsFile(file: File) = copy(sinkFile = Some(file))

  def errorsToFile(file: File) = copy(errorsFile = Some(file))

  def warningsToFile(file: File) = copy(warningsFile = Some(file))

  def sourceFormat(rdfLang: RDFLang) = copy(inFormat = rdfLang)

  def outputFormat(rdfLang: RDFLang) = copy(outFormat = rdfLang)

  def additionalParserChecks(doChecks: Boolean) = copy(parsingsChecks = doChecks)

  def routeWarningsToErrors(warningsToErrors: Boolean) = copy(warningsToErrors = warningsToErrors)

  def parallel(threads: Int) = copy(parallelism = threads)

  def linesPerChunk(lines: Int) = copy(linesChunkSize = lines)
}

class CountingStreamRDF(delegate: StreamRDF) extends StreamRDF {

  val tripleQuadCount = Atomic(0L)

  override def start(): Unit = delegate.start()

  override def triple(triple: graph.Triple): Unit = {

    tripleQuadCount += 1
    delegate.triple(triple)
  }

  override def quad(quad: Quad): Unit = {

    tripleQuadCount += 1
    delegate.quad(quad)
  }

  override def base(base: String): Unit = delegate.base(base)

  override def prefix(prefix: String, iri: String): Unit = delegate.prefix(prefix, iri)

  override def finish(): Unit = delegate.finish()
}

sealed trait ChunkedProcessingResult

case class ParsingStats(triplesRead: Long = 0, warnings: Long = 0, errors: Long = 0) extends ChunkedProcessingResult

case class ReserializedTriples(bytes: Array[Byte]) extends ChunkedProcessingResult

case class WarningBytes(bytes: Array[Byte]) extends ChunkedProcessingResult

case class ErrorBytes(bytes: Array[Byte]) extends ChunkedProcessingResult
