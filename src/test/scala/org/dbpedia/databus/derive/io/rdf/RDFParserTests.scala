package org.dbpedia.databus.derive.io.rdf

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import better.files.File
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.scalatest.FunSuite

import scala.io.Source

class RDFParserTests extends FunSuite {

  test("NTripleParser_remove_WARNINGS") {

    val testFile = File("/todo")
    val firstTripleOS = new ByteArrayOutputStream()
    val firstReportOS = new ByteArrayOutputStream()

    NTripleParser.parse(testFile.newFileInputStream,firstTripleOS,firstReportOS,removeWarnings = true)

    val firstReportLines = Source.fromBytes(firstReportOS.toByteArray,"UTF-8").getLines()
    assert(
      firstReportLines.hasNext,
      "Unnecessary test, no errors found in triples :P"
    )
    firstReportOS.close()

    val parsedFile = new ByteArrayInputStream(firstTripleOS.toByteArray)
    firstTripleOS.close()

    val secondTripleOS = new ByteArrayOutputStream()
    val secondReportOS = new ByteArrayOutputStream()

    NTripleParser.parse(parsedFile,secondTripleOS,secondReportOS,removeWarnings = true)

    val secondReportLines = Source.fromBytes(secondReportOS.toByteArray,"UTF-8").getLines()
    assert(
      secondReportLines.isEmpty,
      "Result contains still bad triples"
    )
    secondReportOS.close()

    val parsedNTriplesBA = secondTripleOS.toByteArray
    val model = ModelFactory.createDefaultModel()

    RDFDataMgr.read(
      model,
      new ByteArrayInputStream(parsedNTriplesBA),
      "urn:base", RDFLanguages.NTRIPLES
    )

    import scala.collection.JavaConversions._
    //imported for listStatements.length

    assert(
      Source.fromBytes(parsedNTriplesBA,"UTF-8").getLines().length == model.listStatements().length,
      "Result contains still bad triples"
    )
  }
}
