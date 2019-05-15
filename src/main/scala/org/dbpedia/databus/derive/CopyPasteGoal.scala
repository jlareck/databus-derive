package org.dbpedia.databus.derive

import java.util

import org.apache.maven.plugin.logging.Log
import org.apache.maven.plugin.AbstractMojo
import org.apache.maven.plugins.annotations._
import org.apache.maven.project.MavenProject

@Mojo(name = "copy-parse")//, defaultPhase = LifecyclePhase.TEST, threadSafe = true)
class CopyPasteGoal extends AbstractMojo {

//  @Parameter(defaultValue = "${project}", readonly = true, required = true)
//  private val project = new MavenProject()

  @Parameter
  private val versions: util.ArrayList[String] = new util.ArrayList[String]

  override def execute(): Unit = {

    List(versions).foreach(println(_))

//    println(s"${project.getGroupId}")

  }

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
