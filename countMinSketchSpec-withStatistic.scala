package com.allegrogroup.reco.sketch

import com.madhukaraphatak.sizeof.SizeEstimator
import org.specs2.mutable.Specification

class CountMinSketchSpec extends Specification {

  "CountMinSketch" should {
    "estimate count" in {
      // given
      val cms = CountMinSketch(30, 500000, Math.abs(System.currentTimeMillis().toInt))
      val limit = 1000000

      // when
      for (i <- 1 until limit) {
        cms.add(i, i)
      }

      // then
      val x = (1 until limit).map { i =>
        (cms.estimateCount(i).toDouble -i) / i
      }
      println(s"CMS size is ${SizeEstimator.estimate(cms)/1024/1024} MB")
      println(s"avg    error rate: ${x.sum/x.size}")
      println(s"median error rate: ${x(x.size/2)}")
      println(s"99th percentile  : ${percentile(99)(x)}")
      success
    }
  }

  def percentile(p: Int)(seq: Seq[Double]): Double = {
    require(0 <= p && p <= 100)                      // some value requirements
    require(seq.nonEmpty)                            // more value requirements
    val sorted = seq.sorted
    val k = math.ceil((seq.length - 1) * (p / 100.0)).toInt
    sorted(k)
  }
}
