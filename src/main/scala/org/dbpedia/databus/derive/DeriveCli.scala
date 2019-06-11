package org.dbpedia.databus.derive

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.dbpedia.databus.derive.io.CustomRdfIO
import scopt._

import scala.sys.process._
import scala.language.postfixOps

case class Config(input: File = null, output: File = null, report: File = null)

object DeriveCli {

  def main(args: Array[String]): Unit = {

//    implicit def betterFileRead = Read.reads(File(_))

    val optionParser = new OptionParser[Config]("foo"){

      head("Line based rdf parser", "0.1")

      arg[File]("<line-based-rdf-file>").required().maxOccurs(1).action((f, p) => p.copy(input = f))

      opt[File]('o', "output").required().maxOccurs(1).action((f, p) =>  p.copy(output = f))

      opt[File]('r', "report").required().maxOccurs(1).action((f, p) =>  p.copy(report = f))

      /*
        -sparkMaster
        -tmpDir
       */

      help("help").text("prints this usage text")
    }

    optionParser.parse(args,Config()) match {
      case Some(config) =>

        val tmpSpark = s"tmp.${java.util.UUID.randomUUID.toString}"

        val spark = SparkSession.builder()
          .appName("FLAT Triple Parser")
          .master("local[*]")
          .config("spark.local.dir",tmpSpark)
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.kryoserializer.buffer.max","512m")
          .getOrCreate()

        val sql = spark.sqlContext

        val tripleReports = CustomRdfIO.parse(
          sql.read.textFile(
            config.input.getAbsolutePath)
            .repartition(Runtime.getRuntime.availableProcessors()*3)
        )(sql).persist(StorageLevel.DISK_ONLY)

        val tripleSink_spark = new File(s"${config.output.getAbsolutePath}.tmp")
        val reportSink_spark = new File(s"${config.report.getAbsolutePath}.tmp")
//
//        tripleReports.foreach( tripleReports =>
//          {
//            if(tripleReports.triple.isDefined) System.out.println(tripleReports.triple.get.toString)
//            if(tripleReports.report.isDefined) System.out.println(tripleReports.report.get)
//          })
//
        CustomRdfIO.writeTripleReports(
          tripleReports = tripleReports,
          Some(tripleSink_spark),
          Some(reportSink_spark)
        )(sql)

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
  }
}

