package org.dbpedia.databus.derive.io

import org.apache.jena.riot.system.ErrorHandler

import scala.collection.mutable.Queue


class NtErrorHandler() extends ErrorHandler {

  private var lastErrors = new Queue[BufferedLine]

  def hasNoErrors: Boolean = {
    lastErrors.isEmpty
  }

  def dequeueError: BufferedLine = {
    lastErrors.dequeue()
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
     lastErrors += BufferedLine(line,s"$level | $message | $col")
  }

  override def warning(message: String, line: Long, col: Long): Unit = logWarning(message, line, col)

  override def error(message: String, line: Long, col: Long): Unit = logError(message, line, col)

  override def fatal(message: String, line: Long, col: Long): Unit = logFatal(message, line, col)
}