package org.apache.mleap.runtime.serialization

import java.io._
import java.nio.charset.Charset
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.mleap.runtime.Transformer
import spray.json._
import scala.language.implicitConversions
import scala.util.Try
import SerializationSupport._

/**
  * Created by hwilkins on 11/13/15.
  */

case class TransformerStringParser(string: String) {
  def parseTransformer(implicit compression: Compression): Try[Transformer] = {
    string.getBytes(TransformerCharaset).parseTransformer
  }
}

case class TransformerByteArrayParser(bytes: Array[Byte]) {
  def parseTransformer(implicit compression: Compression): Try[Transformer] = {
    new ByteArrayInputStream(bytes).parseTransformer
  }
}

case class TransformerStreamParser(stream: InputStream) {
  def parseTransformer(implicit compression: Compression): Try[Transformer] = {
    Try({val realStream = compression match {
      case Compression.Gzip => new GZIPInputStream(stream)
      case Compression.None => stream
    }
      val bytes = new Array[Byte](realStream.available())
      realStream.read(bytes)
      new String(bytes, TransformerCharaset).parseJson.convertTo[Transformer]})
  }
}

case class TransformerFileParser(file: File) {
  def parseTransformer(implicit compression: Compression): Try[Transformer] = {
    new FileInputStream(file).parseTransformer
  }
}

case class TransformerSerializer(transformer: Transformer) {
  def serializeToString(implicit compression: Compression,
                        pretty: PrettyPrint): String = {
    new String(serializeToBytes, TransformerCharaset)
  }

  def serializeToBytes(implicit compression: Compression,
                       pretty: PrettyPrint = PrettyPrint.Default): Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    serializeToStream(stream)
    stream.toByteArray
  }

  def serializeToFile(file: File)
                     (implicit compression: Compression,
                      pretty: PrettyPrint) = {
    serializeToStream(new FileOutputStream(file))
  }

  def serializeToStream(stream: OutputStream)
                       (implicit compression: Compression,
                        pretty: PrettyPrint) = {
    val bytes: Array[Byte] = pretty match {
      case PrettyPrint.True => transformer.toJson.prettyPrint.getBytes(TransformerCharaset)
      case PrettyPrint.False => transformer.toJson.compactPrint.getBytes(TransformerCharaset)
    }
    val realStream = compression match {
      case Compression.Gzip => new GZIPOutputStream(stream)
      case Compression.None => stream
    }
    realStream.write(bytes)
  }

  def toOutputStream(implicit compression: Compression,
                     pretty: PrettyPrint) = {
    val stream = new ByteArrayOutputStream()
    stream.write(transformer.serializeToBytes)
    stream
  }
}

trait SerializationSupport extends RuntimeJsonSupport {
  val TransformerCharaset = Charset.forName("UTF-8")

  sealed trait PrettyPrint
  object PrettyPrint {
    object False extends PrettyPrint
    object True extends PrettyPrint
    val Default = True
  }

  sealed trait Compression
  object Compression {
    object Gzip extends Compression
    object None extends Compression
    val Default = None
  }

  implicit val defaultCompression = Compression.Default
  implicit val defaultPrettyPrint = PrettyPrint.Default

  implicit def transformerSerialize(transformer: Transformer): TransformerSerializer = {
    TransformerSerializer(transformer)
  }

  implicit def stringParseTransformer(string: String): TransformerStringParser = {
    TransformerStringParser(string)
  }

  implicit def byteArrayParseTransformer(bytes: Array[Byte]): TransformerByteArrayParser = {
    TransformerByteArrayParser(bytes)
  }

  implicit def inputStreamParseTransformer(stream: InputStream): TransformerStreamParser = {
    TransformerStreamParser(stream)
  }

  implicit def fileParseTransformer(file: File): TransformerFileParser = {
    TransformerFileParser(file)
  }
}
object SerializationSupport extends SerializationSupport
