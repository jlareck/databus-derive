package org.dbpedia.databus.wagon

import java.io.File
import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Paths}
import java.util.Base64

import org.dbpedia.databus.wagon.auth.{BaseAuth, WagonAuth}
import scalaj.http.Http


/**
 * TODO https://en.wikipedia.org/wiki/WebDAV
 *
 * @param remote
 * @param credentials
 */
class WebDavWagon(remote: String, credentials: BaseAuth) extends Wagon {

  override val auth: WagonAuth = credentials

  override def toString: String = s"$remote"

  override def put(file: File, target: String, replace: Boolean): Boolean = {
    put(Files.readAllBytes(Paths.get(file.getAbsolutePath)), target, replace)
  }

  override def put(data: Array[Byte], target: String, replace: Boolean): Boolean = {
    val httpResponse = Http(remote + target).auth(credentials.getUser, credentials.getPsw).put(data).asString
    httpResponse.code.toString.startsWith("2")
  }

  /**
   * TODO PUT with InputStream other then Array[Byte] possible?
   * Otherwise file is hold in memory before upload...
   *
   * @return
   */
  def put_plainJava(): Boolean = {

    import java.io.OutputStreamWriter
    import java.nio.charset.StandardCharsets

    val url = new URL("http://example.org");
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]

    val auth = "nutzer" + ":" + "password"
    val encodedAuth = Base64.getEncoder.encode(auth.getBytes(StandardCharsets.UTF_8))
    val authHeaderValue = "Basic " + new String(encodedAuth)
    connection.setRequestMethod("PUT")
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "text/plain")
    connection.setRequestProperty("Authorization", authHeaderValue)

    val osw = new OutputStreamWriter(connection.getOutputStream)
    osw.write("Hello")
    osw.flush()
    osw.close()
    val responseCode = connection.getResponseCode
    responseCode.toString.startsWith("2")
  }

  override def list(): Either[List[String], Exception] = {
    Left(List())
  }

  override def exists(): Boolean = {
    false
  }

  override def delete(): Boolean = {
    false
  }
}
