import better.files.File
import org.junit.Test

import scala.collection.parallel.ForkJoinTaskSupport

class FileTests {

  @Test
  def concatBz2(): Unit = {

//    val dir = new File("example/mappingbased-literals/2019.03.01/mappingbased-literals_lang=ca.ttl.bz2.report.spark")
//
//    val find = s"find $dir/ -name part*" !!
//
//    val concat = s"cat $find" #> new File("concat") !


//    var str =  cmd #> new File("files.bz2") !

//    println(str)
  }

  @Test
  def re(): Unit ={

    val rege ="\\.[^.]*$".r

    println(rege.replaceAllIn("a/b/c/","."))

  }

  @Test
  def streamTest(): Unit = {


//    val sparkLocalDir = new File("localSpark/")
//
//
//    val spark = SparkSession.builder()
//      .appName("FLAT Triple Parser")
//      .master("local[*]")
//      .config("spark.local.dir", sparkLocalDir.getAbsolutePath)
//      .config("spark.sql.warehouse.dir", new File(sparkLocalDir , "spark-warehouse").getAbsolutePath)
//      .config("spark.sql.streaming.checkpointLocation", new File(sparkLocalDir, "streaming-checkpoints").getAbsolutePath)
////      .config("spark.local.dir",tmpSpark)
////            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .getOrCreate()
//
//    import spark.sqlContext.implicits._
//
//    val path = "geo-coordinates_reduce=dbpw_resolve=preference.ttl.bz2"
//
//    val st = spark.readStream.textFile(path).map("#"+_)
//      .writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//
//    st.awaitTermination()


//      .trigger(Trigger.Once())
//      .outputMode(OutputMode.Append())
//      .format("text")
//      .option("path", "test")
//      .option("compression", "bzip2")
//      .start()
//
//    st.awaitTermination()

  }

  @Test
  def parTest(): Unit = {
    val pc = Runtime.getRuntime.availableProcessors()

    val l = (0 to 60).par
    l.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(8))
    l.foreach( i => {
      println(fib1(i))
    })

  }

  def fib1(n: Int): Int = n match {
    case 0 | 1 => n
    case _ => fib1(n - 1) + fib1(n - 2)
  }

  @Test
  def aa(): Unit = {
    println("#")
  }

  @Test
  def agg(): Unit = {

//    println(Math.ceil(5/4.0))
//
//    val is = new FileInputStream(new File("/home/marvin/tmpfs/geo-coordinates-mappingbased_lang=de.ttl.bz2"))
//
//
//    val l = List("a.nt.bz2","b.ttl.gz","index.html")
//    val reg = """(.*\.nt.*)|(.*.ttl.*)""".r
//    l.filter(reg.findFirstMatchIn(_).isDefined).foreach(println(_))
//
//    val template = """(\d*)x(\d*)""".r
//    "4x2" match {
//      case template(parFiles,par) => println("matched")
//      case _ => println("wrong")
//    }
  }

  @Test
  def bb(): Unit = {
    val file = File("/home/marvin/workspace/active/databus-derive/data/mappingbased-objects-uncleaned_lang=ca.ttl.bz2")


    println(file.changeExtensionTo(".no"))
//    println(file.nameWithoutExtension)
  }
}
