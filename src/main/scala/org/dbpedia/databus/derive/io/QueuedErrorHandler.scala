package org.dbpedia.databus.derive.io

import org.apache.jena.riot.system.ErrorHandler

import scala.collection._

/**
  * @author Marvin Hofer
  *         Apache Jena RDF ErrorHandler memorizing the last logged reports.
  */
class QueuedErrorHandler() extends ErrorHandler {

  private var reports = new mutable.Queue[RowObject[String]]

  def isQueueEmpty: Boolean = {
    reports.isEmpty
  }

  def nextReport: RowObject[String] = {
    reports.dequeue()
  }

  /** report a warning */
  def logWarning(message: String, line: Long, col: Long): Unit = {
    log("wrn",message,line,col)
  }

  /** report an error */
  def logError(message: String, line: Long, col: Long): Unit = {
    log("err",message,line,col)
  }

  /** report a catastrophic error */
  def logFatal(message: String, line: Long, col: Long): Unit = {
    log("ftl",message,line,col)
  }

  def log(level: String, message: String, line: Long, col: Long): Unit = {
     reports += RowObject(line,s"$level@$col $message ")
  }

  override def warning(message: String, line: Long, col: Long): Unit = logWarning(message, line, col)

  override def error(message: String, line: Long, col: Long): Unit = logError(message, line, col)

  override def fatal(message: String, line: Long, col: Long): Unit = logFatal(message, line, col)
}