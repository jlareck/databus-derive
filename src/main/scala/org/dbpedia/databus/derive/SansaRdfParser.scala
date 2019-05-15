package org.dbpedia.databus.derive

import net.sansa_stack.rdf.spark.io.{ErrorParseMode, NTripleReader, WarningParseMode}
import org.apache.spark.sql.SparkSession

class SansaRdfParser {

  def main(args: Array[String]): Unit = {

    val filePath = "/home/marvin/workspace/DBpedia/databus/repo/enwiki-20190101-mappingbased-literals.ttl.bz2"
    val worker = "4"; // * for all (threads)

    val spark = SparkSession.builder()
      .master(s"local[$worker]")
      .appName("Test")
      .config("spark.local.dir","/tmp")
      .getOrCreate()

    val s = System.currentTimeMillis()

    println(s"parsed: ${NTripleReader.load(spark,filePath,ErrorParseMode.SKIP,WarningParseMode.SKIP).count()}")

    println(s"time: ${(System.currentTimeMillis()-s)/1000}s")
  }
}