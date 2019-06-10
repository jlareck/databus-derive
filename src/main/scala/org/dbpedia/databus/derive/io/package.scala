package org.dbpedia.databus.derive

import java.io.ByteArrayOutputStream
import java.util.Collections

import net.sansa_stack.rdf.spark.io.ntriples.JenaTripleToNTripleString
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.jena.graph.Triple
import org.apache.jena.riot.RDFDataMgr
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

package object io {

  case class RowObject[T](pos: Long, line: T)

  case class TripleReport(triple: Option[Triple], report: Option[String] ) {
    override def toString: String = s"${triple.getOrElse("")}${report.getOrElse("")}}"
  }

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
}
