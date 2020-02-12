package org.dbpedia.databus.wagon

import org.dbpedia.databus.wagon.auth.BaseAuth

import scala.util.control.NonFatal

/**
 * TODO
 */

object WagonBuilder {

  class WagonBuildException(message: String, cause: Throwable = None.orNull) extends Exception(message: String, cause: Throwable)

  def webdav(): WebDavWagonBuilder = {
    new WebDavWagonBuilder()
  }

  class WebDavWagonBuilder() {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    def url(url: String): WebDavWagonBuilder = config("url", url)

    def auth(user: String, password: String): WebDavWagonBuilder = {
      config("user", user).config("password", password)

    }

    def config(key: String, value: String): WebDavWagonBuilder = synchronized {
      options += key -> value
      this
    }

    @throws(classOf[WagonBuildException])
    def build(): WebDavWagon = {
      try {
        new WebDavWagon(
          options("url"),
          new BaseAuth(options("user"), options("url"))
        )
      } catch {
        case NonFatal(e) => throw new WagonBuildException("WebDavWagon: " + e.getMessage)
      }
    }
  }

}