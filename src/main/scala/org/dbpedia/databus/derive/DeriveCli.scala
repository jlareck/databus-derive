package org.dbpedia.databus.derive

import java.util.NoSuchElementException

import scopt._
import better.files.File

object DeriveCli {

  def main(args: Array[String]): Unit = {

//    implicit def betterFileRead = Read.reads(File(_))

    val optionParser = new OptionParser[Foo]("foo"){

      head("Line based rdf parser", "0.1")

      arg[String]("<line-based-rdf-file>").required().maxOccurs(1).action((f, p) => p.setT0(f))

      opt[String]('o', "output-triple").maxOccurs(1).action((s, p) =>  p.setT1(s))

      opt[String]('r', "parse-report").maxOccurs(1).action((s, p) =>  p.setT1(s))

      /*
      -sparkMaster
      -tmpDir
       */

      help("help").text("prints this usage text")
    }

    optionParser.parse(args,Foo()).orNull.run()

  }
}
