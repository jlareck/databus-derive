package org.dbpedia.databus.derive

import java.io.{ByteArrayOutputStream, File}
import java.net.URL
import java.util

import org.apache.commons.io.FileUtils
import org.apache.jena.query._
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat, RDFLanguages}
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations._
import org.apache.spark.sql.SparkSession
import org.dbpedia.databus.derive.download.FileDownloader
import org.dbpedia.databus.sparql.DataidQueries

import scala.collection.JavaConverters._

@Mojo(name = "clone-parse")//, defaultPhase = LifecyclePhase.TEST, threadSafe = true)
class CloneParseGoal extends AbstractMojo {

//  @Parameter(defaultValue = "${project}", readonly = true, required = true)
//  private val project = new MavenProject()

  private val endpoint : String = "https://databus.dbpedia.org/repo/sparql"

  @Parameter(defaultValue = "${session.executionRootDirectory}", readonly = true)
  val sessionRoot: File = null

  @Parameter(defaultValue = "${project.groupId}", readonly = true)
  val groupId: String = null

  @Parameter(defaultValue = "${project.artifactId}", readonly = true)
  val artifactId: String = null

//  @Parameter(defaultValue = "${project.version}", readonly = true)
//  val version: String = null

  @Parameter(defaultValue = "${project.build.directory}", readonly = true)
  val buildDirectory: File = null

  @Parameter
  val versions: util.ArrayList[String] = new util.ArrayList[String]

  override def execute(): Unit = {

    if ( artifactId == "group-metadata" ) {

      versions.asScala.foreach(version => {

        println(s"Looking for version: $version")

        val (pomUrl,artifactId,versionId,files) = handleVersion(version)

        val downloadDir = new File(buildDirectory,s"$artifactId/$versionId")

        downloadPreData(pomUrl,files,downloadDir)

//        parsePreData(downloadDir)
      })
    }
  }

  // TODO change string based operations to jena queries
  /**
    * Retrieve meta information of released dataset version
    * @param version databus version iri
    */
  def handleVersion(version: String): (String,String,String,List[String]) = {

    val query: Query = QueryFactory.create(DataidQueries.queryGetDataidFromVersion(version))
    val results = QueryExecutionFactory.sparqlService(endpoint,query)

    val dataidUrl = results.execSelect().next().getResource("dataset").getURI

    val dataidModel: Model = RDFDataMgr.loadModel(dataidUrl,RDFLanguages.NTRIPLES)

    val dataidUrlParts = dataidUrl.split("/").dropRight(1)
    val versionId = dataidUrlParts.reverse.head
    val artifactId = dataidUrlParts.reverse(1)
    val pomUrl = s"${dataidUrlParts.dropRight(1).mkString("/")}/pom.xml"

    val filesQuery = QueryFactory.create(DataidQueries.queryDatasetFileUrls())
    val filesResults = QueryExecutionFactory.create(filesQuery,dataidModel)
    val files = filesResults.execSelect().asScala.map(_.getResource("file").getURI).toList

    println(info(dataidUrl,pomUrl,artifactId,versionId,files.length))

    (pomUrl,artifactId,versionId,files)
  }


  /**
    *
    * @param pomUrl url of associated pom file
    * @param files list of download urls of single files
    * @param sinkDir directory to download data for further work
    */
  def downloadPreData(pomUrl: String, files: List[String], sinkDir: File): Unit = {

    sinkDir.mkdirs()

    files.foreach(file => {

      val url = new URL(file)

      val tmpFile = new File(sinkDir,new File(url.getPath).getName)

      if(! tmpFile.exists() ) FileDownloader.downloadUrlToFile(url , tmpFile)

    })

    val pomFile = new File(sinkDir.getParentFile,"pom.xml")

    if( ! pomFile.exists()) FileDownloader.downloadUrlToFile(new URL(pomUrl), pomFile)
  }

  def parsePreData(sourceDir: File): Unit = {

    val worker = "*"

    val sparkSession = SparkSession.builder()
      .master(s"local[$worker]")
      .appName("Test")
      .config("spark.local.dir",s"$sourceDir/spark-local-dir/")
      .getOrCreate()
  }

  def info(dataidUrl: String, pom: String, artifact: String, version: String, files: Int) : String =
    s"""
       |Found dataset at: $dataidUrl
       |artifact: $artifact version: $version
       |pom at: $pom
       |files: $files
       |""".stripMargin


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
