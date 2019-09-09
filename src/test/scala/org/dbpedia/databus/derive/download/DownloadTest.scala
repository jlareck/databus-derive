package org.dbpedia.databus.derive.download


import better.files.File
import org.apache.jena.riot.system.IRIResolver
import org.dbpedia.databus.derive.io.findFilePathsInDirectory
import org.dbpedia.databus.derive.mojo.CloneGoal
import org.scalatest.FunSuite

class DownloadTest extends FunSuite {

  test("Clone dataid version into directory") {

    //todo
   // val version =  "https://databus.dbpedia.org/marvin/mappings/specific-mappingbased-properties/2019.07.01"

    //DatabusDownloader.cloneVersionToDirectory(IRIResolver.iriFactory().construct(version),File("target/surefire-reports/test/repo"))
  }

  test("Dev Test") {
    //todo
    //println(new CloneGoal().downloadDirectory.getAbsolutePath)
  }

  test("List Files") {
    //todo
    //findFilePathsInDirectory(File("example/.download").toJava,Array[String]("*/*/*")).foreach(println)

  }
}
