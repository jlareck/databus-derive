package org.dbpedia.databus

import java.net.URL

import better.files.File
import org.apache.jena.query.{Query, QueryExecutionFactory, QueryFactory}
import org.apache.jena.rdf.model.Model
import org.apache.jena.riot.{RDFDataMgr, RDFLanguages}
import org.dbpedia.databus.sparql.DataidQueries

import scala.collection.JavaConverters._

package object derive {

  private val endpoint : String = "https://databus.dbpedia.org/repo/sparql"


  def downloadVersion(url: URL): Unit = {


  }
}
