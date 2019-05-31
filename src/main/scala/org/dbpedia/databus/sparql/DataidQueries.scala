package org.dbpedia.databus.sparql

object DataidQueries {

  def queryDatasetArtifact(): String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?artifact {
       |  ?dataset a dataid:Dataset;
       |           dataid:artifact ?artifact .
       |}
    """.stripMargin

  def queryDatasetFileUrls(): String =
    """
      |PREFIX dcat: <http://www.w3.org/ns/dcat#>
      |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
      |SELECT ?file {
      |  ?dataset a dataid:Dataset;
      |           dcat:distribution/dcat:downloadURL ?file .
      |}
    """.stripMargin

  def queryGetDataidFromVersion(version: String) : String =
    s"""
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#>
       |SELECT ?dataset {
       |  ?dataset a dataid:Dataset;
       |           dataid:version <$version>
       |}
    """.stripMargin
}
