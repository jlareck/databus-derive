package org.dbpedia.databus.derive.io.xml

import java.io.{File, FileInputStream, FileWriter}

import org.apache.maven.model.io.xpp3.{MavenXpp3Reader, MavenXpp3Writer}
import org.codehaus.plexus.util.DirectoryScanner

object PomUtils {

  def copyAllAndChangeGroup(srcDir: File, destDir: File, groupId: String): Unit = {

    val parentPomFile = new File(destDir, "/pom.xml")

    if (! parentPomFile.exists() ) {
      // TODO Create Simple GroupID pom
    }
    findFilePathsInDirectory(srcDir, Array[String]("*/pom.xml")).foreach(

      relativePomPath => {

        copyAndChangeGroup(
          srcPom = new File(srcDir,relativePomPath),
          destPom = new File(destDir,relativePomPath),
          groupId = groupId
        )

        addModuleToPom(parentPomFile, relativePomPath.split("/").dropRight(1).last )
      }
    )
  }

  def copyAndChangeGroup(srcPom: File, destPom: File, groupId: String): Unit = {

    val artifactPom = new MavenXpp3Reader().read(new FileInputStream(srcPom))
    artifactPom.getParent.setGroupId(groupId)
    artifactPom.setGroupId(groupId)

    new MavenXpp3Writer().write(new FileWriter(destPom), artifactPom)
  }

  def addModuleToPom(pom: File, moduleName: String): Unit = {

    val groupPom = new MavenXpp3Reader().read(new FileInputStream(pom))
    groupPom.addModule(moduleName)

    new MavenXpp3Writer().write(new FileWriter(pom), groupPom)
  }

  def findFilePathsInDirectory(baseDir: File, wildcards: Array[String],
                               caseSensitive: Boolean = false): Array[String] = {

    //TODO more scala like
    val directoryScanner = new DirectoryScanner()
    directoryScanner.setIncludes(wildcards)
    directoryScanner.setBasedir(baseDir.getAbsolutePath)
    directoryScanner.setCaseSensitive(caseSensitive)
    directoryScanner.scan()
    directoryScanner.getIncludedFiles
  }
}
