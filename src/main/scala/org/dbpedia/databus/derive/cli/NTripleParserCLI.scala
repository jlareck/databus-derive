package org.dbpedia.databus.derive.cli

import java.io._

import better.files.File
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.dbpedia.databus.derive.io.compress.CompressIO
import org.dbpedia.databus.derive.io.rdf.{NTripleParser, ReportFormat}
import scopt._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.postfixOps
import scala.util.matching.Regex

/**
  * @author Marvin Hofer
  *         Runs FlatRDFTripleParser.parse/write from command line
  */

object NTripleParserCLI {

  case class FlatRDFTripleParserConfig(input: File = null, output: Option[File] = None, report: Option[File] = None,
                                      parFiles: Int = 1, parChunks: Int = 3, chunkSize: Int = 200, compression: Boolean = true,
                                      reportformat: ReportFormat.Value = ReportFormat.TEXT, discardWarning: Boolean = false)

  implicit def betterFileRead: Read[File] = Read.reads(File(_))

  def main(args: Array[String]): Unit = {

    val optionParser: OptionParser[FlatRDFTripleParserConfig] = {

      new OptionParser[FlatRDFTripleParserConfig]("fastparse"){

        head("Line based rdf parser", "0.2")

        arg[File]("<line-based-rdf-file>").required().maxOccurs(1).action((f, p) => p.copy(input = f))
          .text("Line based rdf FILE (DIR parse all containing files)")

        opt[File]('o', "outputFile").maxOccurs(1).action((f, p) =>  p.copy(output = Some(f)))
          .text("Clean triple FILE/DIR (EMPTY for StdOut)")

        opt[File]('r', "reportFile").maxOccurs(1).action((f, p) =>  p.copy(report = Some(f)))
          .text("Validation report FILE/DIR (EMPTY for StdErr)")

        opt[Unit]("rdfReports").maxOccurs(1).action((_,p) => p.copy(reportformat = ReportFormat.RDF))
          .text("Prints a rdf structured validation report")

        opt[Unit]('x', "no-compression").maxOccurs(1).action((_,p) => p.copy(compression = false))
          .text("Disable GZIP compression for output files")

        opt[Unit]( "discard-warnings").maxOccurs(1).action((_,p) => p.copy(discardWarning = true))
          .text("discard-warnings")

        val parTemplate: Regex = """^(\d*)x(\d*)$""".r()
        opt[String]('p',"parallel").maxOccurs(1).action((t,p) => {
          t match { case parTemplate(x,y) => p.copy(parChunks = x.toInt).copy(parFiles = y.toInt) }
        }).validate(t =>
          if (parTemplate.findFirstIn(t).isDefined) success
          else failure("Wrong --parallel {chunks}x{files}")
        ).text("{A}x{B}. A = number of parallel files and B = number of parallel chunks per file. (def 1x3)")

        opt[Int]("chunkSize").maxOccurs(1).action((cs,p) => p.copy(chunkSize = cs))
            .text("Lines per chunk")

        help("help").text("prints this usage text")
      }
    }


    optionParser.parse(args,FlatRDFTripleParserConfig()) match {

      case Some(config) =>

        System.err.println(
          """[INFO] ----------------------------------------
            |[INFO]  Line Based RDF Validation (PURE SCALA)
            |[INFO] ----------------------------------------"""
            .stripMargin)

        if ( config.input.isDirectory ) {

          val rFilter = """(.*\.nt.*)|(.*.ttl.*)""".r

          val pool = config.input.list.filter(f => rFilter.findFirstIn(f.name).isDefined).toArray.par

          pool.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.parFiles))

          pool.foreach( file => {

            System.err.println(s"[INFO] Validating ${file.name}")

            var tF: Option[File] = None
            if(config.output.isDefined) {
              config.output.get.createDirectoryIfNotExists()
              tF = Some(File(config.output.get,file.name))
            }
            val tOS = getOrElseOS(tF,config.compression)(System.out)

            var rF: Option[File] = None
            if(config.report.isDefined) {
              config.report.get.createDirectoryIfNotExists()
              rF = Some(File(config.report.get,file.name))
            }
            val rOS = getOrElseOS(rF,config.compression)(System.err)

            parseFile(file, tOS, rOS, config.parChunks, config.chunkSize, config.reportformat, removeWarnings = config.discardWarning)
          })
        } else {

          System.err.println(s"[INFO] Validating ${config.input.name}")

          val tOS = getOrElseOS(config.output, config.compression)(System.out)
          val rOS = getOrElseOS(config.report, config.compression)(System.err)

          parseFile(config.input, tOS, rOS, config.parChunks, config.chunkSize, config.reportformat, removeWarnings = config.discardWarning)
        }
      case _ => optionParser.showUsage()
    }
  }

  def getOrElseOS(file: Option[File], compression: Boolean)(fallback:PrintStream): OutputStream = {

    if (file.isDefined && compression)
//      new BZip2CompressorOutputStream(file.get.newOutputStream,1)
      new GzipCompressorOutputStream(file.get.newOutputStream)
    else if (file.isDefined)
      file.get.newOutputStream
    else fallback
  }

  //TODO move to FlatRDFTripleParser
  def parseFile(file: File, parsedOS: OutputStream, reportOS: OutputStream,
                par: Int, chunkS: Int, reportFormat: ReportFormat.Value,
                removeWarnings: Boolean = false): Unit ={

    val fIS = file.newFileInputStream

      val cIS = {
        CompressIO.decompressStreamAuto(
          new BufferedInputStream(fIS)
        )
      }

      try {
        NTripleParser.parse(cIS, parsedOS, reportOS, par, chunkS, reportFormat, removeWarnings = removeWarnings)
      }catch {
        case e:IOException => System.err.println(s"Caught IOException, skip file $file")
      }
  }
}

