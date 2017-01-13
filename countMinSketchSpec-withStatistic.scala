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
        val estimate = cms.estimateCount(i)
        val error = (estimate.toDouble -i) / i

        (error, i, estimate)
      }
      println(s"CMS size is ${SizeEstimator.estimate(cms)/1024/1024} MB")
      println(s"avg    error rate: ${x.map(_._1).sum/x.size}")
      println(s"median error rate: ${x(x.size/2)}")
      (90 to 99).foreach { p =>
        println(s"${p}th percentile  : ${percentile(p)(x)}")
      }
      success
    }
  }

  def percentile(p: Int)(seq: Seq[(Double, Int, Long)]): (Double, Int, Long) = {
    require(0 <= p && p <= 100)                      // some value requirements
    require(seq.nonEmpty)                            // more value requirements
    val sorted = seq.sortBy(_._1)
    val k = math.ceil((seq.length - 1) * (p / 100.0)).toInt
    sorted(k)
  }
}
