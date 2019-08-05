package org.dbpedia.databus.derive

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileWriter}
import java.util.Collections

import net.sansa_stack.rdf.spark.io.ntriples.JenaTripleToNTripleString
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.maven.model.io.xpp3.{MavenXpp3Reader, MavenXpp3Writer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.codehaus.plexus.util.DirectoryScanner

import scala.language.postfixOps
import scala.sys.process._

package object io {

  /**
    *
    * @param targetDir
    * @param file
    */
  def cleanFiles(targetDir: File, file: File): Unit = {

    val tripleSink_spark = new File(targetDir,s"${file.getName}.tmp")
    val rerpotSink_spark = new File(targetDir,s"${file.getName}.invalid.tmp")

    val findTriples = s"find ${tripleSink_spark.getAbsolutePath}/ -name part*" !!
    val concatTriples = s"cat $findTriples" #> new File(targetDir,file.getName) !

    if( concatTriples == 0 ) FileUtils.deleteDirectory(tripleSink_spark)
    else System.err.println(s"[WARN] failed to merge ${file.getName}")

    val findReports = s"find ${rerpotSink_spark.getAbsolutePath}/ -name part*" !!
    val concatReports = s"cat $findReports" #> new File(targetDir,s"${file.getName}.invalid") !

    if( concatReports == 0 ) FileUtils.deleteDirectory(rerpotSink_spark)
    else System.err.println(s"[WARN] failed to merge ${file.getName}.invalid")

    // TODO cv api needed
  }

  /**
    *
    * @return
    */
  def compression: Class[BZip2Codec] = classOf[org.apache.hadoop.io.compress.BZip2Codec]

  implicit class RDFWriter[T](triples: RDD[Triple]) {

    val converter = new JenaTripleToNTripleString()

    def saveAsNTriplesFile(path: String, mode: SaveMode = SaveMode.ErrorIfExists, exitOnError: Boolean = false): Unit = {

      val fsPath = new Path(path)
      val fs = fsPath.getFileSystem(triples.sparkContext.hadoopConfiguration)

      val doSave = if (fs.exists(fsPath)) {
        mode match {
          case SaveMode.Append =>
            sys.error(s"Append mode is not supported by ${this.getClass.getCanonicalName} !")
            if (exitOnError) sys.exit(1)
            false
          case SaveMode.Overwrite =>
            fs.delete(fsPath, true)
            true
          case SaveMode.ErrorIfExists =>
            sys.error(s"Given path $path already exists!")
            if (exitOnError) sys.exit(1)
            false
          case SaveMode.Ignore => false
          case _ =>
            throw new IllegalStateException(s"Unsupported save mode $mode ")
        }
      } else {
        true
      }
      import scala.collection.JavaConverters._
      // save only if there was no failure with the path before
      if (doSave) triples
        .mapPartitions(p => {
          val os = new ByteArrayOutputStream()
          RDFDataMgr.writeTriples(os, p.asJava)
          Collections.singleton(new String(os.toByteArray)).iterator().asScala
        })
        .saveAsTextFile(path, compression)
    }
  }

  def copyModulePom(sourceDir: File, targetDir: File, newGroupId: String): Unit = {

  }

  def addModuleToGroupPom(pom: File, module: String): Unit ={

    val reader = new MavenXpp3Reader
    val groupPom = reader.read(new FileInputStream(pom))

    groupPom.addModule(module)

    val writer = new MavenXpp3Writer
    writer.write(new FileWriter(pom), groupPom)
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
