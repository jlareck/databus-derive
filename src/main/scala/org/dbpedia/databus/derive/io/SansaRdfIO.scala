package org.dbpedia.databus.derive.io

import java.io.{ByteArrayOutputStream, File}

import net.sansa_stack.rdf.spark.io.{ErrorParseMode, NTripleReader, WarningParseMode}
import org.apache.commons.io.FileUtils
import org.apache.jena.riot.RDFFormat
import org.apache.jena.graph.Triple
import org.apache.jena.riot.system.{ErrorHandlerFactory, StreamRDFWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.slf4j.Logger

import scala.io.{Codec, Source}

object SansaRdfIO {

  def errMode: ErrorParseMode.Value = ErrorParseMode.SKIP
  def wrnMode: WarningParseMode.Value = WarningParseMode.SKIP
  def checkRDFTerms: Boolean = true
  def logger: Logger = ErrorHandlerFactory.noLogger

  def parseNtriples(file: File)(implicit sparkSession: SparkSession): RDD[Triple] ={

    NTripleReader.load(sparkSession,file.getAbsolutePath,errMode,wrnMode,checkRDFTerms,logger)

  }

  def writeNTriples(dataset: RDD[Triple], sinkFile: File)
                   (implicit sqlContext: SQLContext): Unit = {

    val sinkPath = sinkFile.getAbsolutePath

    val compression = classOf[org.apache.hadoop.io.compress.BZip2Codec]

    toNtripleLinesIterator(dataset).coalesce(1)
      .saveAsTextFile(s"$sinkPath.parsed",compression)

    // workaround of spark folder structure to databus structure
    sinkFile.delete()
    FileUtils.moveFile(new File(s"$sinkPath.parsed/part-00000.bz2"),sinkFile)
    FileUtils.deleteDirectory(new File(s"$sinkPath.parsed"))
  }


  def toNtripleLinesIterator(dataset: RDD[Triple])
                           (implicit sqlContext: SQLContext): RDD[String] = {

    dataset.mapPartitions{ valuesIter: Iterator[org.apache.jena.graph.Triple] =>

      valuesIter.grouped(1000).flatMap { tripleGroup =>

        val bufferingOS = new ByteArrayOutputStream()

        val streamRDFWriter = StreamRDFWriter.getWriterStream(bufferingOS,RDFFormat.NTRIPLES_UTF8)

        streamRDFWriter.start()
        tripleGroup.foreach(streamRDFWriter.triple)
        streamRDFWriter.finish()

        Source.fromBytes(bufferingOS.toByteArray)(Codec.UTF8).getLines()
      }
    }
  }
}

//sourceDir.listFiles().filter(_.isFile).foreach(file => {
//
//println(s" parsing: ${file.getAbsolutePath}")
//
//if( file.getName.contains(".nt") || file.getName.contains(".ttl") ) {
//
//val log = LoggerFactory.getLogger("org.dbpedia.databus.derive")
//
//var triples =  NTripleReader.load(sparkSession,file.getAbsolutePath,ErrorParseMode.SKIP,WarningParseMode.SKIP, false, log)
//
//writeNTriples(triples, s"$targetDir/${file.getName}")(sqlContext)
//
//}
//})
