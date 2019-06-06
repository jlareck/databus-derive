package org.dbpedia.databus.derive

import cats.effect.IO


object DeriveCli {

  def main(args: Array[String]): Unit = {

    var r = 0
    while(r != -1) {
      r = System.in.read()
      println(r)
    }

//    val readLn = IO(scala.io.StdIn.readLine)
//
//    println(readLn.unsafeRunSync())
//
//    val eff = Stream.eval(IO { println("BEING RUN!!"); 1 + 1 })

  }
}
