//package org.dbpedia.databus
//
//import java.io._
//import java.net.{HttpURLConnection, URL, URLConnection}
//
//import org.apache.commons.io.IOUtils
//import org.apache.commons.io.input.CountingInputStream
//import org.apache.jena.rdf.model.ResourceFactory
//import org.apache.jena.riot.system.RiotLib
//import org.junit.Test
//import org.slf4j.LoggerFactory
//
//import sys.process._
//
//class DownloadTests {
//
//  @Test
//  def download(): Unit = {
//
//    val node = RiotLib.factoryRDF.createBlankNode("_:abc");
//
//    println(node.toString())
//
////    val s = "curl http://v122.de/index.html" !!
////
////    println(s)
//  }
//
//  @Test
//  def streamDownload(): Unit = {
//    val FILE_URL = "http://v122.de"
//    val FILE_URL2 = "http://downloads.dbpedia.org/repo/lts/enrichment/mappingbased-objects-uncleaned/2019.03.01/mappingbased-objects-uncleaned_lang%3dca.ttl.bz2"
//
//    val in: BufferedInputStream = new BufferedInputStream(new URL(FILE_URL2).openStream ())
//
//    val cis = new CountingInputStream(in)
//
//
//
//    val filePath = "/home/marvin/workspace/DBpedia/databus/repo/enwiki-20190101-mappingbased-literals.ttl.bz2"
//    val size = new URL(FILE_URL2).openConnection().getContentLengthLong / 1024.0f / 1024
//
//
//    var bts = new Array[Byte](2024)
//    var nRead: Int = -1
//
////    println(cis.getByteCount)
//    while ( { nRead = cis.read(bts, 0, bts.length); -1 !=  nRead }) {
//      val per = (cis.getByteCount/1024.0f/1024)/size*100
//      println("%.2f".format(per))
//    }
//
//
//
//
////
////
////    println(s"$size mb")
////    println(scala.io.Source.fromInputStream(in).getLines().mkString("\n"))
//  }
//
//  @Test
//  def downloadSingleFile(): Unit = {
//
//    val url = new URL("http://downloads.dbpedia.org/repo/lts/enrichment/mappingbased-objects-uncleaned/2019.03.01/mappingbased-objects-uncleaned_lang%3dca.ttl.bz2")
//
//      val conn = url.openConnection()
//
//
//
////    downloadFile(new URL("http://downloads.dbpedia.org/repo/lts/enrichment/mappingbased-objects-uncleaned/2019.03.01/mappingbased-objects-uncleaned_lang%3dca.ttl.bz2"), new File("mappingbased-objects-uncleaned_lang=ca.ttl.bz2"))
//  }
//
//
//  /**
//    * Use "index.html" if URL ends with "/"
//    */
//  def targetName(url : URL) : String = {
//    val path = url.getPath
//    var part = path.substring(path.lastIndexOf('/') + 1)
//    if (part.nonEmpty) part else "index.html"
//  }
//
//  /**
//    * Download file from URL to given target file.
//    */
//  def downloadFile(url : URL, file : File) : Unit = {
//    val conn = url.openConnection
//    try {
//      downloadFile(conn, file)
//    } finally conn match {
//      // http://dumps.wikimedia.org/ seems to kick us out if we don't disconnect.
//      case conn: HttpURLConnection => conn.disconnect
//      // But only disconnect if it's a http connection. Can't do this with file:// URLs.
//      case _ =>
//    }
//  }
//
//  /**
//    * Download file from URL to given target file.
//    */
//  protected def downloadFile(conn: URLConnection, file : File): Unit = {
//    val in = inputStream(conn)
//    try
//    {
//      val out = outputStream(file)
//      try
//      {
//        IOUtils.copy(in, out)
//      }
//      finally out.close
//    }
//    finally in.close
//  }
//
//  /**
//    * Get input stream. Mixins may decorate the stream or open a different stream.
//    */
//  protected def inputStream(conn: URLConnection) : InputStream = conn.getInputStream
//
//  /**
//    * Get output stream. Mixins may decorate the stream or open a different stream.
//    */
//  protected def outputStream(file: File) : OutputStream = new FileOutputStream(file)
//
//
//  @Test
//  def counting(): Unit = {
//
//    val url = new URL("http://downloads.dbpedia.org/repo/lts/enrichment/mappingbased-literals/2019.03.01/mappingbased-literals_lang%3dca.ttl.bz2")
//    val conn = url.openConnection()
//
////    val cis = new MyInputStream(conn.getInputStream,conn.getContentLengthLong,1L << 21)
////
////    val fos = new FileOutputStream(new File(url.getFile).getName)
////
////    IOUtils.copy(cis,fos)
//  }
//
//  @Test
//  def te(): Unit = {
//    val progressStep: Long = 1L << 21
//    println(progressStep)
//    println(1L<<1)
//  }
//
//  @Test
//  def test(): Unit = {
//    import org.slf4j.Logger
//    import org.slf4j.LoggerFactory
//
//    val riotLoggerName = "org.apache.jena.riot"
//    val  riotLogger = LoggerFactory.getLogger(riotLoggerName)
//
//    riotLogger.error("test")
//  }
//}
