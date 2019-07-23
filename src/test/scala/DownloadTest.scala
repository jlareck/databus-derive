
import better.files.File
import org.apache.jena.graph.NodeFactory
import org.apache.jena.iri.IRIFactory
import org.apache.jena.rdf.model.ResourceFactory
import org.apache.jena.riot.system.IRIResolver
import org.dbpedia.databus.derive.CloneParseGoal
import org.dbpedia.databus.derive.download.DatabusDownloader
import org.dbpedia.databus.sparql.DataidQueries
import org.scalatest.FunSuite

class DownloadTest extends FunSuite {

  test("Clone dataid version into directory") {

    val version =  "https://databus.dbpedia.org/marvin/mappings/specific-mappingbased-properties/2019.07.01"

    DatabusDownloader.cloneVersionToDirectory(IRIResolver.iriFactory().construct(version),File("repo"))
  }

  test("Dev Test") {

    println(new CloneParseGoal().downloadDirectory.getAbsolutePath)
  }
}
