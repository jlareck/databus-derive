package org.dbpedia.databus.derive.cli

import java.io._

import better.files.File
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}
import org.dbpedia.databus.derive.io.rdf.{FlatRDFTripleParser, ReportFormat}
import scopt._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.postfixOps
import scala.util.matching.Regex

/**
  * @author Marvin Hofer
  *         Runs FlatRDFTripleParser.parse/write from command line
  */

object FlatRDFTripleParserCLI {

  case class FlatRDFTripleParserConfig(input: File = null, output: Option[File] = None, report: Option[File] = None,
                                      parFiles: Int = 1, par: Int = 3, chunkS: Int = 200, compression: Boolean = true,
                                      reportformat: ReportFormat.Value = ReportFormat.TEXT)

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

        val parTemplate: Regex = """^(\d*)x(\d*)$""".r()
        opt[String]('p',"parallel").maxOccurs(1).action((t,p) => {
          t match { case parTemplate(x,y) => p.copy(par = x.toInt).copy(parFiles = y.toInt) }
        }).validate(t =>
          if (parTemplate.findFirstIn(t).isDefined) success
          else failure("Wrong --parallel {chunks}x{files}")
        ).text("{A}x{B}. A = number of parallel files and B = number of parallel chunks in file. (def 3x1)")

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

            parseFile(file, tOS, rOS, config.par, config.chunkS, config.reportformat)
          })
        } else {

          System.err.println(s"[INFO] Validating ${config.input.name}")

          val tOS = getOrElseOS(config.output, config.compression)(System.out)
          val rOS = getOrElseOS(config.report, config.compression)(System.err)

          parseFile(config.input, tOS, rOS, config.par, config.chunkS, config.reportformat)
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

  def parseFile(file: File, tOS: OutputStream, rOS: OutputStream,
                par: Int, chunkS: Int, reportFormat: ReportFormat.Value): Unit ={

    val fIS = file.newInputStream

    try {

      val cis = {
        new CompressorStreamFactory()
          .createCompressorInputStream(
            new BufferedInputStream(fIS))
      }

      FlatRDFTripleParser.parse(cis, tOS, rOS, par, chunkS, reportFormat)

    } catch {

      case ce: CompressorException =>
        System.err.println(s"[WARN] No compression found for ${file.name} - raw input")
        FlatRDFTripleParser.parse(fIS, tOS, rOS, par, chunkS, reportFormat)

      case unknown: Throwable => println("[ERROR] Unknown exception: " + unknown)
    }
  }
}

