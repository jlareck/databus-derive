//package org.dbpedia.databus.derive
//
//import org.apache.maven.plugin.{AbstractMojo, Mojo}
//import org.apache.maven.plugins.annotations.Parameter
//
//import java.io.File
//import java.net.URL
//import org.apache.maven.plugin.logging.Log
//
//
///**
//  * Collection of all properties
//  *
//  * Dev Note:
//  * val targetDirectory = new File (mavenTargetDirectory,"/databus/"+artifactId+"/"+version)
//  * or scripting does not work as these are executed on startup, the injection of values
//  * by maven is done later, so all vars are empty on startup
//  *
//  */
//trait Parameters extends Mojo {
//
//  this: AbstractMojo =>
//
//
//  /** ***********************************
//    * CODE THAT WILL BE EXECUTED BEFORE RUNNING EACH MOJO
//    * ************************************/
//  {
//    Properties.printLogoOnce(getLog)
//  }
//
//  /**
//    * Project vars given by Maven
//    */
//
//  @Parameter(defaultValue = "${project.groupId}", readonly = true)
//  val groupId: String = null
//
//  @Parameter(defaultValue = "${project.artifactId}", readonly = true)
//  val artifactId: String = null
//
//  @Parameter(defaultValue = "${project.version}", readonly = true)
//  val version: String = null
//
//  @Parameter(defaultValue = "${project.packaging}", readonly = true)
//  val packaging: String = null
//
//  @Parameter(defaultValue = "${project.build.directory}", readonly = true)
//  val buildDirectory: File = null
//
//  @Parameter(defaultValue = "${project.build.finalName}", readonly = true)
//  val finalName: String = null
//
////  @Parameter(defaultValue = "${settings}", readonly = true)
////  val settings: Settings = null
//
//  @Parameter(defaultValue = "${session.executionRootDirectory}", readonly = true)
//  val sessionRoot: File = null
//
//
//  /**
//    * Project internal parameters
//    */
//
////  /**
////    * Input folder for data, defaultValue "src/main/databus/${project.version}"
////    * Each artifact (abstract dataset identity) consists of several versions of the same dataset
////    * These versions are kept in all in parallel subfolders
////    * Tipp: src/main is the maven default, if you dislike having three folders you can also use "databus/${project.version}"
////    */
////  // done, good defaults
////  @Parameter(property = "databus.inputDirectory", defaultValue = ".", required = true)
////  val inputDirectory: File = null
////
////  @Parameter(property = "databus.insertVersion") val insertVersion: Boolean = true
////
////
//  /**
//    * common variables used in the code
//    */
//
//
//  def isParent(): Boolean = {
//    packaging.equals("pom")
//  }
//
//
//}
//
///**
//  * Static property object, which contains all static code
//  */
//object Properties {
//
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
//}
