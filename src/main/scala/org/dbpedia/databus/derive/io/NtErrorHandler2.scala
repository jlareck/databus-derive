package org.dbpedia.databus.derive.io

import org.apache.jena.riot.system.ErrorHandler

class NtErrorHandler2() extends ErrorHandler {

  /** report a warning */
  def logWarning(message: String, line: Long, col: Long): Unit = {
    println(s"wrn2 | $message | $line | $col")
//    if (log != null) log.warn(fmtMessage(message, line, col))
  }

  /** report an error */
  def logError(message: String, line: Long, col: Long): Unit = {
    println(s"err2 | $message | $line | $col")

    //    if (log != null) log.error(fmtMessage(message, line, col))
  }

  /** report a catastrophic error */
  def logFatal(message: String, line: Long, col: Long): Unit = {
    println(s"ftl2 | $message | $line | $col")
    //    if (log != null) logError(message, line, col)
  }

  override def warning(message: String, line: Long, col: Long): Unit = logWarning(message, line, col)

  override def error(message: String, line: Long, col: Long): Unit = logError(message, line, col)

  override def fatal(message: String, line: Long, col: Long): Unit = logFatal(message, line, col)
}