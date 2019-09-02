package org.dbpedia.databus.derive.io.compress

import java.io.InputStream

import org.apache.commons.compress.compressors.{CompressorException, CompressorStreamFactory}

object CompressIO {

  def decompressStreamAuto(stream: InputStream): InputStream = {

    try {

      new CompressorStreamFactory().createCompressorInputStream(
        CompressorStreamFactory.detect(stream),
        stream,
        true
      )

    } catch {

      case ce: CompressorException =>
        System.err.println(s"[WARN] No compression found for input stream - raw input")
        stream

      case unknown: Throwable => println("[ERROR] Unknown exception: " + unknown)
        stream
    }
  }
}
