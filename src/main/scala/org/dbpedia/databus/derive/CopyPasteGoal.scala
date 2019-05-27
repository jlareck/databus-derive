package org.dbpedia.databus.derive

import java.io.File
import java.net.URL
import java.util

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hdfs.server.common.JspHelper.Url
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations._

import scala.collection.JavaConverters._

@Mojo(name = "copy-parse")//, defaultPhase = LifecyclePhase.TEST, threadSafe = true)
class CopyPasteGoal extends AbstractMojo {

//  @Parameter(defaultValue = "${project}", readonly = true, required = true)
//  private val project = new MavenProject()

  private val endpoint : String = "https://databus.dbpedia.org/repo/sparql"


  @Parameter
  val versions: util.ArrayList[String] = new util.ArrayList[String]

  @Parameter(defaultValue = "${project.build.directory}", readonly = true)
  val buildDirectory: File = null


  override def execute(): Unit = {

    println(buildDirectory.getAbsolutePath)

    versions.asScala.foreach(version => {

      println(s"Looking for version: $version")



    })

  }

  def handleVersion(version: String): Unit = {

    val query: Query = QueryFactory.create(queryGetDataidFromVersion(version))

    val results = QueryExecutionFactory.sparqlService(endpoint,query)

    val dataidUrl = results.execSelect().next().getResource("dataset").getURI

    val dataidModel: Model = RDFDataMgr.loadModel(dataidUrl,RDFLanguages.NTRIPLES)

    val (pomUrl,artifactId,versionId,files) = getMetaValues(dataidUrl, dataidModel)

    val tmpDir = new File(s"${buildDirectory.getAbsolutePath}/$artifactId/$versionId")

    clonePreData(pomUrl, files, tmpDir)

    val targetDir = new File("")

    parsePreData(tmpDir, targetDir)

  }

  def parsePreData(sourceDir: File, targetDir: File): Unit ={

  }

  def clonePreData(pomUrl: String, files: List[String], tmpDir: File): Unit = {

    tmpDir.mkdirs()

    files.foreach(file => {

      val url = new URL(file)
      val tmpFile = new File(s"${tmpDir.getAbsolutePath}/${new File(url.getPath).getName}")

      if(! tmpFile.exists() ) FileUtils.copyURLToFile(url , tmpFile)

    })

    val pomFile = new File(tmpDir.getParentFile,"pom.xml")
    if( ! pomFile.exists()) FileUtils.copyURLToFile(new URL(pomUrl), pomFile)
  }

  def getMetaValues(dataidUrl: String, dataidModel: Model) : (String, String, String, List[String]) = {

    val dataidUrlParts = dataidUrl.split("/").dropRight(1)

    val versionId = dataidUrlParts.reverse.head

    val artifactId = dataidUrlParts.reverse(1)

    val pomUrl = s"${dataidUrlParts.dropRight(1).mkString("/")}/pom.xml"

    val filesQuery = QueryFactory.create(queryDatasetFileUrls())
    val filesResults = QueryExecutionFactory.create(filesQuery,dataidModel)
    val files = filesResults.execSelect().asScala.map(_.getResource("file").getURI).toList

    println(info(dataidUrl,pomUrl,artifactId,versionId,files.length))

    (pomUrl,artifactId,versionId,files)
  }

  def info(dataidUrl: String, pom: String, artifact: String, version: String, files: Int) : String =
    s"""
       |Found dataset at: $dataidUrl
       |artifact: $artifact version: $version
       |pom at: $pom
       |files: $files
       |""".stripMargin


  def queryDatasetArtifact(): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?artifact {
       |  ?dataset a dataid:Dataset;
       |           dataid:artifact ?artifact .
       |}
    """.stripMargin

  def queryDatasetFileUrls(): String =
    """
      |PREFIX dcat: <http://www.w3.org/ns/dcat#>
      |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
      |SELECT ?file {
      |  ?dataset a dataid:Dataset;
      |           dcat:distribution/dcat:downloadURL ?file .
      |}
    """.stripMargin

  def queryGetDataidFromVersion(version: String) : String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?dataset {
       |  ?dataset a dataid:Dataset;
       |           dataid:version <$version>
       |}
    """.stripMargin

//  val pluginVersion = "1.3-SNAPSHOT"
//
//  var logoPrinted = false
//
//  //NOTE: NEEDS TO BE COMPATIBLE WITH TURTLE COMMENTS
//  val logo =
//    s"""|
//        |
//        |######
//        |#     #   ##   #####   ##   #####  #    #  ####
//        |#     #  #  #    #    #  #  #    # #    # #
//        |#     # #    #   #   #    # #####  #    #  ####
//        |#     # ######   #   ###### #    # #    #      #
//        |#     # #    #   #   #    # #    # #    # #    #
//        |######  #    #   #   #    # #####   ####   ####
//        |
//        |# Plugin version ${pluginVersion} - https://github.com/dbpedia/databus-maven-plugin
//        |
//        |""".stripMargin
//
//  def printLogoOnce(mavenlog: Log) = {
//    if (!logoPrinted) {
//      mavenlog.info(logo)
//    }
//    logoPrinted = true
//  }
}
