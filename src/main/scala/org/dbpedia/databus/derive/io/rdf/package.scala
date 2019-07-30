package org.dbpedia.databus.derive.io

package object rdf {

  case class RowObject[T](pos: Long, line: T)
}
