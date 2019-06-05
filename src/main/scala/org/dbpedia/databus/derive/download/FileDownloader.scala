package org.dbpedia.databus.derive.download

import java.io.{File, FileOutputStream}
import java.net.URL

import org.apache.commons.io.{FileUtils, IOUtils}

object FileDownloader {

  def downloadUrlToFile(url: URL, file: File): Unit = {

//    FileUtils.copyURLToFile(url,file)

    println(s"download: ${url.getPath}")

    val conn = url.openConnection()

    val cis = new LoggingInputStream(conn.getInputStream,conn.getContentLengthLong, 1L << 19)

    val fos = new FileOutputStream(file)

    try {
      IOUtils.copy(cis,fos)
    } finally {
      fos.close()
      cis.close()
    }
  }
}
