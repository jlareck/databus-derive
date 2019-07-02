package org.dbpedia.databus.derive

import java.io.{BufferedInputStream, FileInputStream}

import better.files.File
import org.apache.commons.compress.compressors
import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory, FileNameUtil}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.derive.io.{CustomRdfIO, FastParse}
import scopt._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.sys.process._
import scala.language.postfixOps


/**
  * @author Marvin Hofer
  *         Runs CustomRdfIo.parse/write from command line
  */

case class FastParseConfig(input: File = null, output: Option[File] = None, report: Option[File] = None,
                           parFiles: Int = 1, par: Int = 3, chunkS: Int = 200)

object FastParseCli {

  implicit def betterFileRead = Read.reads(File(_))

  def main(args: Array[String]): Unit = {

    val optionParser = new OptionParser[FastParseConfig]("fastparse"){

      head("Line based rdf parser", "0.2")

      arg[File]("<line-based-rdf-file>").required().maxOccurs(1).action((f, p) => p.copy(input = f))

      opt[File]('o', "output-file").maxOccurs(1).action((f, p) =>  p.copy(output = Some(f)))

      opt[File]('r', "report-file").maxOccurs(1).action((f, p) =>  p.copy(report = Some(f)))

      opt[String]('p',"parallel").maxOccurs(1).action((t,p) => {

        val parTemplate = """(\d*)x(\d*)""".r()
        t match {
          case parTemplate(x,y) =>
            p.copy(par = x.toInt).copy(parFiles = y.toInt)
        }
      })

      help("help").text("prints this usage text")
    }

    optionParser.parse(args,FastParseConfig()) match {
      case Some(config) =>

        System.err.println(
          """[INFO] ----------------------------------------
            |[INFO]  Line Based RDF Validation (PURE SCALA)
            |[INFO] ----------------------------------------"""
            .stripMargin)

        val i = config.input

        if ( i.isDirectory ) {


          val rFilter = """(.*\.nt.*)|(.*.ttl.*)""".r
          val pool = i.toJava.listFiles().filter(f => rFilter.findFirstIn(f.getName).isDefined).par
          pool.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(config.parFiles))

          pool.foreach( f => {

            System.err.println(s"[INFO] Validating ${f.getName}")

            val fis = new FileInputStream(f)
            val oos = {
              if (config.output.isDefined)
                File(config.output.get, s"${f.getName}.out").newOutputStream
              else System.out
            }
            val ros = {
              if (config.report.isDefined)
                File(config.report.get,s"${f.getName}.out").newOutputStream
              else System.err
            }

            try {

              val cis = {
                new CompressorStreamFactory()
                  .createCompressorInputStream(
                    new BufferedInputStream(
                      new FileInputStream(f)))
              }

              FastParse.parse(cis,oos,ros,config.par,200)

            } catch {

              case ce: CompressorException =>
                System.err.println(s"[WARN] No compression found for ${f.getName} - raw input")
                FastParse.parse(fis,oos,ros,config.par,200)
            }
          })
        } else {

          val f = config.input.toJava

          System.err.println(s"[INFO] Validating ${f.getName}")

          val fis = new FileInputStream(f)
          val oos = {
            if (config.output.isDefined)
              File(config.output.get, s"${f.getName}.out").newOutputStream
            else System.out
          }
          val ros = {
            if (config.report.isDefined)
              File(config.report.get, s"${f.getName}.out").newOutputStream
            else System.err
          }

          try {

            val cis = {
              new CompressorStreamFactory()
                .createCompressorInputStream(
                  new BufferedInputStream(
                    new FileInputStream(f)))
            }

            FastParse.parse(cis, oos, ros, config.par, 200)

          } catch {

            case ce: CompressorException =>
              System.err.println(s"[WARN] No compression found for ${f.getName} - raw input")
              FastParse.parse(fis, oos, ros, config.par, 200)
          }
        }

      case _ => optionParser.showUsage()
    }
  }

}

