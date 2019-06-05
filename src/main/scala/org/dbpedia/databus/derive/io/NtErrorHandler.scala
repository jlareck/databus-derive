package org.dbpedia.databus.derive.io

import org.apache.jena.riot.SysRIOT
import org.apache.jena.riot.SysRIOT.fmtMessage
import org.apache.jena.riot.system.ErrorHandler
import org.slf4j.Logger

class NtErrorHandler() extends ErrorHandler {

  /** report a warning */
  def logWarning(message: String, line: Long, col: Long): Unit = {
    println(s"wrn | $message | $line | $col")
//    if (log != null) log.warn(fmtMessage(message, line, col))
  }

  /** report an error */
  def logError(message: String, line: Long, col: Long): Unit = {
    println(s"err | $message | $line | $col")

    //    if (log != null) log.error(fmtMessage(message, line, col))
  }

  /** report a catastrophic error */
  def logFatal(message: String, line: Long, col: Long): Unit = {
    println(s"ftl | $message | $line | $col")
    //    if (log != null) logError(message, line, col)
  }

  override def warning(message: String, line: Long, col: Long): Unit = logWarning(message, line, col)

  override def error(message: String, line: Long, col: Long): Unit = logError(message, line, col)

  override def fatal(message: String, line: Long, col: Long): Unit = logFatal(message, line, col)
}