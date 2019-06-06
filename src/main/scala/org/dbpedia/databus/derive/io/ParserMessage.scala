package org.dbpedia.databus.derive.io

trait ParserMessage {

  def message: String

  def line: String

  def lineIndex: Long

  def column: Int

  def logLinePrefix: String

  override def toString: String = {

    def tabEscapedMessage = message.replace('\t', ' ')

    s"$logLinePrefix@$lineIndex,$column: $tabEscapedMessage\t$line"
  }
}

trait ParseError extends ParserMessage

case class Warning(message: String, line: String, lineIndex: Long, column: Int) extends ParserMessage {

  override def logLinePrefix: String = "WARNING"
}

case class Error(message: String, line: String, lineIndex: Long, column: Int) extends ParseError {

  override def logLinePrefix: String = "ERROR"
}

case class Fatal(message: String, line: String, lineIndex: Long, column: Int) extends ParseError {

  override def logLinePrefix: String = "FATAL"
}
