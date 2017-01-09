import java.io.UnsupportedEncodingException
import java.util.Random

import scala.util.hashing.MurmurHash3


object CountMinSketch {

  def apply(depth: Int, width: Int, seed: Int): CountMinSketch = {
    val eps = 2.0 / width
    val confidence = 1 - 1 / Math.pow(2, depth)
    new CountMinSketch(CountMinSketchParameters(depth, width, seed, eps, confidence))
  }

  def apply(eps: Double, confidence: Double, seed: Int): CountMinSketch = {
    val width: Int = Math.ceil(2 / eps).toInt
    val depth: Int = Math.ceil(-Math.log(1 - confidence) / Math.log(2)).toInt
    new CountMinSketch(CountMinSketchParameters(depth, width, seed, eps, confidence))
  }
}

private case class CountMinSketchParameters(depth: Int, width: Int, seed: Int, eps: Double, confidence: Double)

/**
  * // TODO should check for overflow in counters?
  *
  * @param parameters
  */
class CountMinSketch(parameters: CountMinSketchParameters) {

  println(parameters)

  private val PRIME_MODULUS: Long = (1L << 31) - 1

  private val table = Array.ofDim[Long](parameters.depth, parameters.width)
  private val hashA: Seq[Long] = {
    val r = new Random(parameters.seed)
    (0 until parameters.depth).map(_ => r.nextInt(Integer.MAX_VALUE).toLong)
  }
  private var size: Long = 0

  def estimateCount(item: Long): Long = {
    var result = Long.MaxValue
    for ( i <- 0 until parameters.depth) {
      result = Math.min(result, table(i)(hash(item, i)))
    }
    result
  }

  def estimateCount(item: String): Long = {
    var result = Long.MaxValue
    val buckets = StringHash.getHashBuckets(item, parameters.depth, parameters.width)
    for( i <- 0 until parameters.depth) {
      result = Math.min(result, table(i)(buckets(i)))
    }
    result
  }

  def getSize = size

  def add(item: Long, value: Int): Unit = {
    for( i <- 0 until parameters.depth) {
      table(i)(hash(item, i)) += value
    }
    size += 1
  }

  def add(item: String, value: Int): Unit = {
    val buckets = StringHash.getHashBuckets(item, parameters.depth, parameters.width)
    for( i <- 0 until parameters.depth) {
      table(i)(buckets(i)) += value
    }
    size += 1
  }

  // copy paste
  def hash(item: Long, i: Int): Int = {
    var hash = hashA(i) * item
    // A super fast way of computing x mod 2^p-1
    // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
    // page 149, right after Proposition 7.
    hash += hash >> 32
    hash &= PRIME_MODULUS
    // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
    hash.toInt % parameters.width
  }
}


object StringHash {

  def getHashBuckets(key: String, hashCount: Int, max: Int): Array[Int] = {
    val bytes = try {
      key.getBytes("UTF-16")
    } catch {
      case e: UnsupportedEncodingException => throw new RuntimeException(e)
    }
    getHashBuckets(bytes, hashCount, max)
  }

  private def getHashBuckets(b: Array[Byte], hashCount: Int, max: Int): Array[Int] = {
    val hash1 = MurmurHash3.arrayHash(b, 0)
    val hash2 = MurmurHash3.arrayHash(b, hash1)
    (0 until hashCount).map { i =>
      Math.abs((hash1 + i * hash2) % max)
    }
  }.toArray
}