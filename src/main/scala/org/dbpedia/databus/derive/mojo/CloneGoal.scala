package org.dbpedia.databus.derive.mojo

import java.{io, util}

import better.files
import better.files.File
import org.apache.commons.io.FileUtils
import org.apache.jena.riot.system.IRIResolver
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations._
import org.dbpedia.databus.derive.cli.NTripleParserCLI.parseFile
import org.dbpedia.databus.derive.download.DatabusDownloader
import org.dbpedia.databus.derive.io.findFilePathsInDirectory
import org.dbpedia.databus.derive.io.rdf.ReportFormat

import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.sys.process.Process

/** @author Marvin Hofer
  *
  * mvn databus-derive:clone-parse
  *
  * MAVEN Goal: retrieve and parse DBpedia databus dataset's by version
  */
@Mojo(name = "clone", defaultPhase = LifecyclePhase.INSTALL, threadSafe = true)
class CloneGoal extends AbstractMojo {

  private val endpoint : String = "https://databus.dbpedia.org/repo/sparql"

  @Parameter(defaultValue = "${session.executionRootDirectory}", readonly = true)
  val sessionRoot: java.io.File = null

  @Parameter(defaultValue = "${project.basedir} ", readonly = true)
  val baseDirectory: java.io.File = null

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

  @Parameter(
    property = "databus.derive.downloadDirectory",
    defaultValue = "${project.build.directory}/databus/derive/downloads"
  )
  val downloadDirectory: java.io.File = null

  @Parameter(
    property = "databus.derive.reportDirectory",
    defaultValue = "${project.build.directory}/databus/derive/reports"
  )
  val reportDirectory: java.io.File = null

  @Parameter(
    property = "databus.derive.packageDirectory",
    defaultValue = "${project.basedir}"
  )
  val packageDirectory: java.io.File = null

  @Parameter(
    property = "databus.derive.skipParsing",
    defaultValue = "false"
  )
  val skipParsing: Boolean = false

  @Parameter(
    property = "databus.derive.deleteDownloadCache",
    defaultValue = "false"
  )
  val deleteDownloadCache: Boolean = false

  @Parameter(
    property = "databus.derive.deleteDownloadCache",
    defaultValue = "${project.build.directory}/databus/derive/.spark_local_dir"
  )
  val sparkLocalDir: java.io.File = null

  @Parameter(
    property = "databus.derive.removeFoundWarnings",
    defaultValue = "true"
  )
  val removeWarnings: Boolean = true

  @Parameter(
    property = "databus.derive.parFiles",
    defaultValue = "4"
  )
  val parFiles: Int = 4

  @Parameter(
    property = "databus.derive.parChunks",
    defaultValue = "8"
  )
  val parChunks: Int = 8

  @Parameter(
    property = "databus.derive.chunkSize",
    defaultValue = "10000"
  )
  val chunkSize: Int = 10000

  override def execute(): Unit = {

    val finalBuildDirectory = new io.File(buildDirectory,"databus/derive/final")

    if ( artifactId == "group-metadata" ) {

      versions.asScala.foreach( versionStr => {

        System.err.println(s"[INFO] Looking for version: $versionStr")
        val versionIRI = IRIResolver.iriFactory().construct(versionStr)

        DatabusDownloader.cloneVersionToDirectory(
          version = versionIRI,
          directory = files.File(downloadDirectory.getAbsolutePath),
          skipFilesIfExists = true
        )

//        PomUtils.copyAllAndChangeGroup(downloadDirectory, finalBuildDirectory, groupId, version)
      })

      if ( skipParsing ) {

        FileUtils.copyDirectory(downloadDirectory, finalBuildDirectory)
      }
      else {

        parseDirectoryToDirectory(
          sourceDirectory = File(downloadDirectory.getAbsolutePath),
          reportDirectory = File(reportDirectory.getAbsolutePath),
          targetDirectory = File(finalBuildDirectory.getAbsolutePath)
        )
      }

      collectReports(reportDirectory,finalBuildDirectory)

//      is now done for each file
//      compressOutputWithBash(finalBuildDirectory)

      FileUtils.copyDirectory(finalBuildDirectory,packageDirectory)
    }
  }

  def collectReports(sourceDirectory: io.File, targetDirectory: io.File): Unit = {

    sourceDirectory.listFiles().foreach(

      artifact => {

        artifact.listFiles().foreach(

          version => {

            val newArtifact = new io.File(targetDirectory,s"${artifact.getName}/${version.getName}")
            newArtifact

            val cmd = {
              Seq(
                "bash",
                "-c",
                s"cat $$(find ${version.getAbsolutePath} -name '*_debug.txt.bz2') " +
                  s"> ${newArtifact.getAbsolutePath}/${artifact.getName}_debug.txt.bz2 ")
            }

            System.err.println(s"[INFO] ${cmd.mkString(" ")}")

            Process(cmd).!
          }
        )
      }
    )

  }

  def compressOutputWithBash(directory: io.File): Int = {

    //TODO postfix operation notation, enable feature

    val cmd = Seq("bash", "-c", s"lbzip2 $$(find ${directory.getAbsolutePath} -regextype posix-egrep -regex '.*\\.(ttl|nt)')")

    System.err.println(s"[INFO] ${cmd.mkString(" ")}")

    Process(cmd).!
  }

  def parseDirectoryToDirectory( sourceDirectory: File,
                                 targetDirectory: File,
                                 reportDirectory: File ): Unit = {

    if(! sourceDirectory.isDirectory) {

      System.err.println(s"[ERROR] $sourceDirectory is no directory")
    }
    else {

      //TODO from pom conf
//      val parFiles = 4
//      val parChunks = 8
//      val chunkSize = 10000
      /*
      reportFormat, removeWarnings
       */

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
          s"${sourceFile.nameWithoutExtension}_debug.txt"

        reportFile.parent.createDirectoryIfNotExists()

        if( rFilter.pattern.matcher(sourceFile.name).matches ) {

          System.err.println(s"[INFO] Parsing ${sourceFile.name}")
          parseFile(
            sourceFile,
            targetFile.newOutputStream,
            reportFile.newOutputStream,
            parChunks,
            chunkSize,
            ReportFormat.TEXT,
            removeWarnings = true
          )

          lbzip2File(targetFile)
          lbzip2File(reportFile)

        }
        else {

          System.err.println(s"[INFO] Skip parsing ${sourceFile.name}")
          FileUtils.moveFile(sourceFile.toJava,targetFile.toJava)
        }
      })
    }
  }

  def lbzip2File(file: File): Unit = {

    val cmd = Seq("bash", "-c", s"lbzip2  ${file.pathAsString}")

    System.err.println(s"[INFO] ${cmd.mkString(" ")}")

    Process(cmd).!
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
