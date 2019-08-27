package org.dbpedia.databus.derive.download


import better.files.File
import org.apache.jena.riot.system.IRIResolver
import org.dbpedia.databus.derive.io.findFilePathsInDirectory
import org.dbpedia.databus.derive.mojo.CloneGoal
import org.scalatest.FunSuite

class DownloadTest extends FunSuite {

  test("Clone dataid version into directory") {

    val version =  "https://databus.dbpedia.org/marvin/mappings/specific-mappingbased-properties/2019.07.01"

    DatabusDownloader.cloneVersionToDirectory(IRIResolver.iriFactory().construct(version),File("repo"))
  }

  test("Dev Test") {

    println(new CloneGoal().downloadDirectory.getAbsolutePath)
  }

  test("List Files") {

    findFilePathsInDirectory(File("example/.download").toJava,Array[String]("*/*/*")).foreach(println)

  }
}