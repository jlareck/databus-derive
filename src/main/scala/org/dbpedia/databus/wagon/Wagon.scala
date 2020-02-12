package org.dbpedia.databus.wagon

import java.io.File

import org.dbpedia.databus.wagon.auth.WagonAuth

trait Wagon {

  val auth: WagonAuth

  def put(ile: File, target: String, replace: Boolean): Boolean

  def put(data: Array[Byte], target: String, replace: Boolean): Boolean

  def list(): Either[List[String], Exception]

  def exists(): Boolean

  def delete(): Boolean
}

object Wagon {

  class WagonException(message: String, cause: Throwable = None.orNull) extends Exception(message: String, cause: Throwable)

}
