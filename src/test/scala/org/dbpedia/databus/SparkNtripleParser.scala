//package org.dbpedia.databus
//
//import javacode.TokenizerTextForgiving2
//import java.io.StringBufferInputStream
//
//import scala.io.Source
//import java.util.Properties
//
//import com.google.common.base.Predicates
//import com.google.common.collect.Iterators
//import net.sansa_stack.rdf.benchmark.io.ReadableByteChannelFromIterator
//import net.sansa_stack.rdf.spark.riot.lang.LangNTriplesSkipBad
//import net.sansa_stack.rdf.spark.riot.tokens.TokenizerTextForgiving
//import org.apache.jena.atlas.io.PeekReader
//import org.apache.jena.atlas.iterator.IteratorResourceClosing
//import org.apache.jena.graph.Triple
//import org.apache.jena.riot.{RDFDataMgr, RIOT, RiotParseException}
//import org.apache.jena.riot.lang.RiotParsers
//import org.apache.jena.riot.system._
//import org.apache.jena.riot.tokens.{TokenizerFactory, TokenizerText}
//import org.apache.jena.util.Tokenizer
//import org.apache.log4j.{LogManager, PropertyConfigurator}
//import org.apache.spark.sql.SparkSession
//import org.junit.Test
//import java.io.ByteArrayInputStream
//import java.io.InputStream
//import java.nio.charset.Charset
//
//import javacode.{LangNTriplesSkipBad2, TokenizerTextForgiving2}
//import org.apache.jena.rdf.model
//import org.apache.jena.rdf.model.ModelFactory
//import org.dbpedia.databus.derive.io.NtErrorHandler
//
//import scala.io.{Codec, Source}
//import scala.reflect.ClassTag
//
//class SparkNtripleParser {
//
//  @Test
//  def riotTest() : Unit = {
//
//  }
//
//  @Test
//  def apaTest(): Unit = {
//
//    val rdfser =
//      "<a> <b > c> ."
//
//    val inputStream = new ByteArrayInputStream(rdfser.getBytes(Charset.forName("UTF-8")))
//
//    val tokenizer = TokenizerFactory.makeTokenizerUTF8(inputStream)
//
//
//    val parserproflie = new ParserProfileStd(
//      RiotLib.factoryRDF(),
//      new NtErrorHandler,
//      IRIResolver.create(),
//      PrefixMapFactory.createForInput(),
//      RIOT.getContext.copy(),
//      true,
//      true
//    )
//
//
//    val handler = new NtErrorHandler
////
////    var p: Node = null
//    val st = tokenizer.next()
//    try {
//      val pt = tokenizer.next()
//      val p =parserproflie.create(null,pt)
//      println("p]")
//    } catch  {
//      case rpe: RiotParseException => handler.warning(rpe.getOriginalMessage,rpe.getCol,rpe.getLine)
//    }
////    val ot = tokenizer.next()
////
////    val s =parserproflie.create(null,st)
////    println("s]")
////
////    val o =parserproflie.create(null,ot)
////    println("o]")
////    var triple: Triple = null
////    try {
////
////      triple = parserproflie.createTriple(s,p,o,st.getLine,st.getColumn)
////    } catch  {
////      case rpe : RiotParseException => rpe.getOriginalMessage
////    }
////    println(triple.toString)
//  }
//
//  @Test
//  def parseFile(): Unit = {
//
////    val properties = new Properties
//    val stream = getClass.getResourceAsStream("/slf4j.properties")
////    properties.load()
////    LogManager.resetConfiguration()
////    PropertyConfigurator.configure(properties)
//
//    val worker = "*"
//
//    val sparkSession = SparkSession.builder()
//      .master(s"local[$worker]")
//      .appName("Test")
//      .config("spark.local.dir","spark-local-dir/")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .getOrCreate()
//
//    val path = "/home/marvin/workspace/active/databus-derive/example/target/mappingbased-literals/2019.03.01/mappingbased-literals_lang=ca.1000.ttl"
//
//    val rdd = sparkSession.sparkContext
//      .textFile(path, minPartitions = 20)
//
//    val profileWrapper = NonSerializableObjectWrapper {
//      val errorHandler = new NtErrorHandler()
//
//      new ParserProfileStd(RiotLib.factoryRDF, errorHandler, IRIResolver.create, PrefixMapFactory.createForInput, RIOT.getContext.copy, true, true)
//    }
//
//    import scala.collection.JavaConverters._
//
//    // parse each partition
//    val cnt = rdd.mapPartitions(p => {
//      // convert iterator to input stream
//      val input = ReadableByteChannelFromIterator.toInputStream(p.asJava)
//
//      // create the parsing iterator
//      val it =
//      {
//
//          // here we "simply" skip illegal triples
//          // we need a custom tokenizer
//          val tokenizer = new TokenizerTextForgiving2(PeekReader.makeUTF8(input))
//          tokenizer.setErrorHandler(new NtErrorHandler)
//
//          // which is used by a custom N-Triples iterator
//          val it = new LangNTriplesSkipBad2(tokenizer, profileWrapper.get, null)
//
//          // filter out null values
//          Iterators.filter(it, Predicates.notNull[Triple]())
//      }
//      new IteratorResourceClosing[Triple](it, input).asScala
//    }).count()
//
//    println(cnt)
//  }
//}
//
//private class NonSerializableObjectWrapper[T: ClassTag](constructor: => T) extends AnyRef with Serializable {
//  @transient private lazy val instance: T = constructor
//
//  def get: T = instance
//}
//
//private object NonSerializableObjectWrapper {
//  def apply[T: ClassTag](constructor: => T): NonSerializableObjectWrapper[T] = new NonSerializableObjectWrapper[T](constructor)
//}
//
