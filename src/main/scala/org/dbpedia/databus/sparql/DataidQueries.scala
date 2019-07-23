package org.dbpedia.databus.sparql

object DataidQueries {

  private val prefixDatabus: String = {
    s"""PREFIX dct:    <http://purl.org/dc/terms/>
       |PREFIX dcat:   <http://www.w3.org/ns/dcat#>
       |PREFIX dataid: <http://dataid.dbpedia.org/ns/core#> """.stripMargin
  }

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

  def queryVersionDownloadUrls(version: String) : String =
    s"""$prefixDatabus
       |SELECT ?dataset ?downloadUrl {
       |  ?dataset a dataid:Dataset ;
       |           dataid:version <$version> ;
       |           dcat:distribution/dcat:downloadURL ?downloadUrl .
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

  def newQuery(version: String): String = {
    s"""
       |$prefixDatabus
       |SELECT ?dataset(GROUP_CONCAT(DISTINCT ?downloadUrl; SEPARATOR=";") AS ?downloadUrls) {
       |
       |  ?dataset a dataid:Dataset;
       |           dataid:version <$version> ;
       |           dcat:distribution/dcat:downloadURL ?downloadUrl .
       |
       |} GROUP BY ?dataset
       |""".stripMargin
  }
}
