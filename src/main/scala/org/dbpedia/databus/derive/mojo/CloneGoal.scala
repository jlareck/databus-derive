package org.dbpedia.databus.derive.mojo

import java.util

import better.files
import org.apache.commons.io.FileUtils
import org.apache.jena.riot.system.IRIResolver
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations._
import org.dbpedia.databus.derive.download.DatabusDownloader
import org.dbpedia.databus.derive.io.xml.PomUtils

import scala.collection.JavaConverters._

/** @author Marvin Hofer
  *
  * mvn databus-derive:clone-parse
  *
  * MAVEN Goal: retrieve and parse DBpedia databus dataset's by version
  */
@Mojo(name = "clone")//, defaultPhase = LifecyclePhase.TEST, threadSafe = true)
class CloneGoal extends AbstractMojo {

//  @Parameter(defaultValue = "${project}", readonly = true, required = true)
//  private val project = new MavenProject()

  private val endpoint : String = "https://databus.dbpedia.org/repo/sparql"

  @Parameter(defaultValue = "${session.executionRootDirectory}", readonly = true)
  val sessionRoot: java.io.File = null

  @Parameter(defaultValue = "${project.groupId}", readonly = true)
  val groupId: String = null

  @Parameter(defaultValue = "${project.artifactId}", readonly = true)
  val artifactId: String = null

  @Parameter(defaultValue = "${project.version}", readonly = true)
  val version: String = null

  @Parameter(defaultValue = "${project.build.directory}", readonly = true)
  val buildDirectory: java.io.File = null

  /**
    * List of dataset version IRIs
    *
    * pom.xml > build > plugins > plugin > configuration > versions > version*
    */
  @Parameter
  val versions: util.ArrayList[String] = new util.ArrayList[String]

  @Parameter
  val downloadDirectory: java.io.File = new java.io.File("./.download")

  @Parameter
  val skipParsing: Boolean = false

  @Parameter
  val repositoryDirectory: java.io.File = new java.io.File("./")

  @Parameter
  val deleteDownloadCache: Boolean = true

  override def execute(): Unit = {

    if ( artifactId == "group-metadata" ) {

      versions.asScala.foreach(version => {

        println(s"Looking for version: $version")
        val versionIRI = IRIResolver.iriFactory().construct(version)

        DatabusDownloader.cloneVersionToDirectory(
          version = versionIRI,
          directory = files.File(downloadDirectory.getAbsolutePath),
          skipFilesIfExists = true
        )

        if ( skipParsing ) {

          FileUtils.copyDirectory(downloadDirectory, repositoryDirectory)
        }
        else {

          println("ParserImpl is missing. skipParsing = true")
          FileUtils.copyDirectory(downloadDirectory, repositoryDirectory)

          //TODO FlatRDFTripleParser.parseDirectoryToDirectory()
        }

        PomUtils.copyAllAndChangeGroup(downloadDirectory, repositoryDirectory, groupId)
      })

      if ( deleteDownloadCache ) FileUtils.deleteDirectory(downloadDirectory)
    }
  }

//  /**
//    * download single files into target/databus/$artifact/$version
//    *
//    * @param pomUrl url of associated pom file
//    * @param files list of download urls of single files
//    * @param sinkDir directory to download data for further work
//    */
//  def downloadPreData(pomUrl: String, files: List[String], sinkDir: File): Unit = {
//
////    sinkDir.mkdirs()
//
//    files.foreach(file => {
//
//      //      val url = new URL(file)
//      //
//      //      val tmpFile = new File(sinkDir,new File(url.getPath).getName)
//      //
//      //      if(! tmpFile.exists() ) FileDownloader.downloadUrlToFile(url , tmpFile)
//      //
//      //    })
//      //
//      //    val pomFile = new File(sinkDir.getParentFile,"pom.xml")
//      //
//      //    if( ! pomFile.exists()) FileDownloader.downloadUrlToFile(new URL(pomUrl), pomFile)
//    })
//  }
//
//  /**
//    * parsing raw databus data using SANSA-Stack
//    *
//    * @param sourceDir directory/file containing raw data
//    * @param targetDir directory/file to write parsed data
//    */
//  def parsePreData(sourceDir: File, targetDir: File): Unit = {
//
//    val worker = "*"
//
//    val spark_local_dir = s"$sourceDir/spark-local-dir/"
//
//    val sparkSession = SparkSession.builder()
//      .master(s"local[$worker]")
//      .appName("Test")
//      .config("spark.local.dir",spark_local_dir)
//      .getOrCreate()
//
//    sourceDir.listFiles().filter(_.isFile).foreach( file => {
//
//      //TODO File handling and naming
//      val tripleReports = CustomRdfIO.parse(
//        sparkSession.sparkContext.textFile(file.getAbsolutePath,Runtime.getRuntime.availableProcessors()*4))
//
//      CustomRdfIO.writeTripleReports(
//        tripleReports,
//        Some(new File(targetDir,s"${file.getName}.tmp")),
//        Some(new File(targetDir,s"${file.getName}.invalid.tmp"))
//      )
//
//      cleanFiles(targetDir,file)
//
//      // Deprecated
//      // SansaRdfIO.writeNTriples(parsed,new File(targetDir,file.getName))(sqlContext = sparkSession.sqlContext)
//      // val parsed = SansaRdfIO.parseNtriples(file)(sparkSession)
//    })
//
//    sparkSession.close()
//
//    FileUtils.deleteDirectory(new File(spark_local_dir))
//  }
//
//
//
//
//  /**
//    * toString of metadata information
//    *
//    * @param dataidUrl URL of corresponding dataid
//    * @param pom URL of corresponding deployment pom
//    * @param artifact dataset artifact
//    * @param version dataset version
//    * @param files list of URLs pointing to related dataset single files
//    * @return
//    */
//  def info(dataidUrl: String, pom: String, artifact: String, version: String, files: Int) : String =
//    s"""
//       |Found dataset at: $dataidUrl
//       |artifact: $artifact version: $version
//       |pom at: $pom
//       |files: $files
//       |""".stripMargin


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
