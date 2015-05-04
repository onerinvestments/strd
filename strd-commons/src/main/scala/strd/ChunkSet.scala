package strd

import parquet.column.values.rle.{RunLengthBitPackingHybridDecoder, RunLengthBitPackingHybridEncoder}
import parquet.bytes.BytesUtils
import strd.bytes.BytesChannelWrapper
import java.nio.ByteBuffer
import scala.util.Random


/**
 *
 * User: light
 * Date: 8/12/13
 * Time: 12:50 PM
 */

trait ChunkSet {
  def size: Int

  def toIterable: Iterable[Long]
}

/*
object RLEBasedChunkSet {
  def apply(list: Seq[Long]) = {
    val chIds = list.map(l => ChunkId.decompress(l))


    if (chIds.isEmpty) {
      new RLEBasedChunkSet(new Array[Byte](0), new Array[Byte](0), 0, 0, 0, 0, 0)
    } else {
      val tableId = chIds.head.table
      val agg = chIds.head.agg

      val seqs = chIds.map(_.seq.toInt)
      val versions = chIds.map(_.ver)

      val bitWidth = BytesUtils.getWidthFromMaxInt(seqs.max)
      val size = seqs.size

      val encoder = new RunLengthBitPackingHybridEncoder(bitWidth, size)
      seqs.foreach(x => encoder.writeInt(x))

      val bitWidthV = BytesUtils.getWidthFromMaxInt(versions.max)
      val sizeV = versions.size

      val encoderV = new RunLengthBitPackingHybridEncoder(bitWidthV, sizeV)
      versions.foreach(x => encoderV.writeInt(x))

      (encoder.toBytes.toByteArray, encoderV.toBytes.toByteArray, size, bitWidth, bitWidthV, tableId, agg)
    }
  }
}
*/

/*
class RLEBasedChunkSet(val bytes: Array[Byte],
                       val vBytes: Array[Byte],
                       val size: Int,
                       val bw: Int,
                       val vBw: Int,
                       val tableId: Int,
                       val agg: Int) extends ChunkSet {


  override def toIterable = new Iterable[Long] {
    def iterator = {

      new Iterator[Long] {
        val count = RLEBasedChunkSet.this.size
        val decoder = new RunLengthBitPackingHybridDecoder( bw, new BytesChannelWrapper( bytes ) )
        val decoderV = new RunLengthBitPackingHybridDecoder( vBw, new BytesChannelWrapper( vBytes) )

        var pointer = 0

        def next() = {
          val seq = decoder.readInt()
          val ver = decoderV.readInt()
          pointer = pointer + 1
          new ChunkId(tableId, agg, seq, ver).compress()
        }

        def hasNext = pointer < count
      }
    }
  }

}
*/

case class ChunkSetByNodes(map: Map[Int, ChunkSet])

object EMPTY_CHUNK_SET extends SeqChunkSet(Nil)

case class SeqChunkSet(seq: Seq[Long]) extends ChunkSet{
  def size = seq.size
  def toIterable = seq
}


