package org.dbpedia.databus.derive.mojo

import java.util

import better.files
import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.riot.system.IRIResolver
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations._
import org.dbpedia.databus.derive.cli.FlatRDFTripleParserCLI.parseFile
import org.dbpedia.databus.derive.download.DatabusDownloader
import org.dbpedia.databus.derive.io.findFilePathsInDirectory
import org.dbpedia.databus.derive.io.rdf.ReportFormat
import org.dbpedia.databus.derive.io.xml.PomUtils

import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool

/** @author Marvin Hofer
  *
  * mvn databus-derive:clone-parse
  *
  * MAVEN Goal: retrieve and parse DBpedia databus dataset's by version
  */
@Mojo(name = "clone", defaultPhase = LifecyclePhase.INSTALL, threadSafe = true)
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
  val targetDirectory: java.io.File = new java.io.File("./")

  @Parameter
  val reportDirectory: java.io.File = new java.io.File("./.reports")

  @Parameter
  val deleteDownloadCache: Boolean = true

  override def execute(): Unit = {

    if ( artifactId == "group-metadata" ) {

      versions.asScala.foreach( versionStr => {

        System.err.println(s"[INFO] Looking for version: $versionStr")
        val versionIRI = IRIResolver.iriFactory().construct(versionStr)

        DatabusDownloader.cloneVersionToDirectory(
          version = versionIRI,
          directory = files.File(downloadDirectory.getAbsolutePath),
          skipFilesIfExists = true
        )

        if ( skipParsing ) {

          FileUtils.copyDirectory(downloadDirectory, targetDirectory)
        }
        else {

          parseDirectoryToDirectory(
            sourceDirectory = File(downloadDirectory.getAbsolutePath),
            targetDirectory = File(targetDirectory.getAbsolutePath),
            reportDirectory = File(reportDirectory.getAbsolutePath)
          )
        }

        PomUtils.copyAllAndChangeGroup(downloadDirectory, targetDirectory, groupId, version)
      })

      if ( deleteDownloadCache ) FileUtils.deleteDirectory(downloadDirectory)
    }
  }

  def parseDirectoryToDirectory( sourceDirectory: File,
                                 targetDirectory: File,
                                 reportDirectory: File ): Unit = {

    if(! sourceDirectory.isDirectory) {

      System.err.println(s"[ERROR] $sourceDirectory is no directory")
    }
    else {

      //TODO from pom conf or defaults
      val parFiles = 2
      val parChunks = 4
      val chunkSize = 200

      val rFilter = """(.*\.nt.*)|(.*\.ttl.*)""".r

      val pool = findFilePathsInDirectory(sourceDirectory.toJava, Array[String]("*/*/*")).par
      pool.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(parFiles))

      pool.foreach( subPath => {

        val targetArtifact = subPath.split("/")(0)
        val targetVersion = subPath.split("/")(1)

        val sourceFile = sourceDirectory / subPath

        val targetFile = targetDirectory / targetArtifact / targetVersion /
          s"${sourceFile.nameWithoutExtension}.ttl"

        targetFile.parent.createDirectoryIfNotExists()

        val reportFile = reportDirectory / targetArtifact / targetVersion /
          s"${sourceFile.nameWithoutExtension}_debug=rdf.ttl"

        reportFile.parent.createDirectoryIfNotExists()

        if( rFilter.pattern.matcher(sourceFile.name).matches ) {

          System.err.println(s"[INFO] Parsing ${sourceFile.name}")
          parseFile(
            sourceFile,
            targetFile.newOutputStream,
            reportFile.newOutputStream,
            parChunks,
            chunkSize,
            ReportFormat.RDF
          )
        }
        else {

          System.err.println(s"[INFO] Skip parsing ${sourceFile.name}")
          FileUtils.moveFile(sourceFile.toJava,targetFile.toJava)
        }
      })
    }
  }

  val pluginVersion = "1.3-SNAPSHOT"

  var logoPrinted = false

  //NOTE: NEEDS TO BE COMPATIBLE WITH TURTLE COMMENTS
  val logo =
    s"""|
        |
        |######
        |#     #   ##   #####   ##   #####  #    #  ####
        |#     #  #  #    #    #  #  #    # #    # #
        |#     # #    #   #   #    # #####  #    #  ####
        |#     # ######   #   ###### #    # #    #      #
        |#     # #    #   #   #    # #    # #    # #    #
        |######  #    #   #   #    # #####   ####   ####
        |
        |# Plugin version ${pluginVersion} - https://github.com/dbpedia/databus-maven-plugin
        |
        |""".stripMargin

//  def printLogoOnce(mavenlog: Log) = {
//    if (!logoPrinted) {
//      mavenlog.info(logo)
//    }
//    logoPrinted = true
//  }
}
