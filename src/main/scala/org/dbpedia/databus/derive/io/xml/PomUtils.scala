package org.dbpedia.databus.derive.io.xml

import java.io.{File, FileInputStream, FileWriter}

import better.files.Resource
import org.apache.maven.model.io.xpp3.{MavenXpp3Reader, MavenXpp3Writer}
import org.dbpedia.databus.derive.io.findFilePathsInDirectory

object PomUtils {

  def copyAllAndChangeGroup(srcDir: File, destDir: File, groupId: String, groupVersion: String): Unit = {

    val parentPomFile = new File(destDir, "pom.xml")

    if (! parentPomFile.exists() ) {
      createDefaultGroupPom(
        parentPomFile,
        groupId,
//      { val versionFormat = new SimpleDateFormat("yyyy.MM.dd");
//      versionFormat.format(Calendar.getInstance().getTime) }
        groupVersion
      )
    }
    findFilePathsInDirectory(srcDir, Array[String]("*/pom.xml")).foreach(

      relativePomPath => {

        System.err.println(s"[INFO] Moving $relativePomPath")
        copyAndChangeParent(
          srcPom = new File(srcDir,relativePomPath),
          destPom = new File(destDir,relativePomPath),
          groupId = groupId,
          groupVersion = groupVersion
        )

        addModuleToPom(parentPomFile, relativePomPath.split("/").dropRight(1).last )
      }
    )
  }

  def copyAndChangeParent(srcPom: File, destPom: File, groupId: String, groupVersion: String): Unit = {

    val artifactPom = new MavenXpp3Reader().read(new FileInputStream(srcPom))
    artifactPom.getParent.setVersion(groupVersion)
    artifactPom.getParent.setGroupId(groupId)
//    artifactPom.setGroupId(groupId) //TODO needed?

    new MavenXpp3Writer().write(new FileWriter(destPom), artifactPom)
  }

  def addModuleToPom(pom: File, moduleName: String): Unit = {

    val groupPom = new MavenXpp3Reader().read(new FileInputStream(pom))
    groupPom.addModule(moduleName)

    new MavenXpp3Writer().write(new FileWriter(pom), groupPom)
  }

  def createDefaultGroupPom(file: File, groupId: String, groupVersion: String): Unit = {

    val pom = new MavenXpp3Reader().read(Resource.getAsStream("template.pom.xml"))

    pom.setGroupId(groupId)
    pom.setArtifactId("group-metadata")
    pom.setVersion(groupVersion)

    new MavenXpp3Writer().write(new FileWriter(file),pom)
  }
}
