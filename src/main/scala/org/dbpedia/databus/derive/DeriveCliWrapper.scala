package org.dbpedia.databus.derive

import java.io.{File, FileInputStream}

object DeriveCliWrapper {

  def main(args: Array[String]): Unit = {

    val is: FileInputStream  = new FileInputStream(new File("example/pom.xml"))
    System.setIn(is)
    DeriveCli.main(args)
  }
}
