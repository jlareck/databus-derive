package org.dbpedia.databus.derive.download

import java.io.FileOutputStream
import java.net.URL

import better.files.File
import org.apache.commons.io.IOUtils

object FileDownloader {

  def downloadUrlToFile(url: URL, file: File, createParentDirectory: Boolean = false): Unit = {

    if( createParentDirectory ) file.parent.createDirectoryIfNotExists()

    System.err.println(s"$url -> $file")

    val conn = url.openConnection()
    val cis = new LoggingInputStream(conn.getInputStream,conn.getContentLengthLong, 1L << 19)
    val fos = new FileOutputStream(file.toJava)

    try {
      IOUtils.copy(cis,fos)
    } finally {
      fos.close()
      cis.close()
    }
  }

  def downloadUrlToDirectory(url: URL, directory: File,
                             createDirectory: Boolean = false, skipIfExists: Boolean = false): Unit = {


    val file = directory / url.getFile.split("/").last
    if( ! ( skipIfExists && file.exists ) ) downloadUrlToFile(url, file, createDirectory)
  }
}
