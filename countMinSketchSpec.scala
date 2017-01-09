import com.madhukaraphatak.sizeof.SizeEstimator // from     "com.madhukaraphatak" %% "java-sizeof" % "0.1"
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
      val x = (1 until limit).flatMap { i =>
        if (cms.estimateCount(i) == i) {
          None
        } else {
          Some(i)
        }
      }
      println(s"CMS size is ${SizeEstimator.estimate(cms)/1024/1024} MB")
      println(s"error rate: ${x.size.toDouble/limit}")
      success
    }
  }
}
