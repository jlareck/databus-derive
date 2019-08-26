package org.dbpedia.databus.derive.cli

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.derive.io.rdf.SansaBasedRDFParser
import scopt._

import scala.language.postfixOps
import scala.sys.process._

//import org.apache.log4j.Level
//import org.apache.log4j.Logger

case class Config(input: File = null, output: File = null, report: File = null)

/**
  * @author Marvin Hofer
  *         Runs CustomRdfIo.parse/write from command line
  */
object DeriveCli {

  def main(args: Array[String]): Unit = {


//    Logger.getLogger("org").setLevel(Level.WARN)
//    Logger.getLogger("akka").setLevel(Level.WARN)

    val optionParser = new OptionParser[Config]("foo"){

      head("Line based rdf parser", "0.1")

      arg[File]("<line-based-rdf-file>").required().maxOccurs(1).action((f, p) => p.copy(input = f))

      opt[File]('o', "output-file").required().maxOccurs(1).action((f, p) =>  p.copy(output = f))

      opt[File]('r', "report-file").required().maxOccurs(1).action((f, p) =>  p.copy(report = f))

      /* TODO
        -sparkMaster
        -tmpDir
       */

      help("help").text("prints this usage text")
    }

    optionParser.parse(args,Config()) match {
      case Some(config) =>

        val tmpSpark = s"tmp.${java.util.UUID.randomUUID.toString}"

        System.err.println(
          """
            |-----------------------------------
            | Line Based RDF Validation (SPARK)
            |-----------------------------------
          """.stripMargin)

        val spark = SparkSession.builder()
          .appName("FLAT Triple Parser")
          .master("local[*]")
          .config("spark.local.dir",tmpSpark)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.kryoserializer.buffer.max","512m")
          .getOrCreate()

        val tripleReports = SansaBasedRDFParser.parse(spark.sparkContext.textFile(
          path = config.input.getAbsolutePath,
          minPartitions = Runtime.getRuntime.availableProcessors()*3
        ))

        System.err.println("Parsed data now writing...")

        val tripleSink_spark = new File(s"${config.output.getAbsolutePath}.tmp")
        val reportSink_spark = new File(s"${config.report.getAbsolutePath}.tmp")

        SansaBasedRDFParser.writeTripleReports(
          tripleReports = tripleReports,
          Some(tripleSink_spark),
          Some(reportSink_spark)
        )

        System.err.println("Cleaning Files...")

        // Using spark process env. to concat files using cat.
        val findTriples = s"find ${tripleSink_spark.getAbsolutePath}/ -name part*" !!
        val concatTriples = s"cat $findTriples" #> config.output !

        if( concatTriples == 0 ) FileUtils.deleteDirectory(tripleSink_spark)
        else System.err.println(s"[WARN] failed to merge ${tripleSink_spark.getName}.tmp/*")

        val findReports = s"find ${reportSink_spark.getAbsolutePath}/ -name part*" !!
        val concatReports = s"cat $findReports" #> config.report !

        if( concatReports == 0 ) FileUtils.deleteDirectory(reportSink_spark)
        else System.err.println(s"[WARN] failed to merge ${reportSink_spark.getName}.tmp/*")

        spark.close()

        FileUtils.deleteDirectory(new File(tmpSpark))

      case _ => optionParser.showUsage()
    }
//    optionParser.parse()
  }
}

