package org.dbpedia.databus.wagon.adapter

import org.dbpedia.databus.wagon.WagonBuilder
import org.scalatest.FunSuite

class WebDavWagonTest extends FunSuite {

  test("WebDav Wagon build") {

    WagonBuilder.webdav().url("").auth("", "").build()
  }
}
