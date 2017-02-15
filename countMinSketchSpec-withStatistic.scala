import com.madhukaraphatak.sizeof.SizeEstimator
import org.specs2.mutable.Specification

class CountMinSketchSpec extends Specification {

  "CountMinSketch" should {
    "estimate count" in {
      // given
      val cms = CountMinSketch(10, 20000, Math.abs(System.currentTimeMillis().toInt))
      println(s"Sketch parameters: ${cms.parameters}")
      val limit = 1000000
      val startTime = System.nanoTime()

      // when
      for (i <- 1 until limit) {
        cms.add("" + i, i)
      }

      // then
      val x = (1 until limit).map { i =>
        val estimate = cms.estimateCount("" + i)
        val error = (estimate.toDouble - i) / i

        Estimate(error, i, estimate)
      }
      println(s"CMS size is ${SizeEstimator.estimate(cms)/1024/1024} MB")
      println(s"CMS size is ${SizeEstimator.estimate(cms)/1024} kB")
      println(s"avg    error rate: ${x.map(_.error).sum/x.size}")
      println(s"median error rate: ${x(x.size/2)}")
      (90 to 99).foreach { p =>
        println(s"${p}th percentile  : ${percentile(p)(x)}")
      }

      println("---------------------")

      val bounds = List(0, 5, 25, 125, 1000, 5000, 10000).map(_.toDouble)
      val histogramBins: List[(Double, List[Estimate])] = Distribution(20, x.toList, Some(bounds)).histogram
      println("Histogram:")
      histogramBins.foreach { case (k, v) =>
        println(f"  [$k%5.2f] -> (${v.size}) ${v.map(_.value).sorted.reverse.take(5).mkString(", ")}")
      }

      println(s"test duration: ${System.nanoTime() - startTime}")
      success
    }
  }

  def percentile(p: Int)(seq: Seq[Estimate]): Estimate = {
    require(0 <= p && p <= 100)
    require(seq.nonEmpty)
    val sorted = seq.sortBy(_.error)
    val k = math.ceil((seq.length - 1) * (p / 100.0)).toInt
    sorted(k)
  }
}

case class Estimate(error: Double, value: Long, estimate: Long)

case class Distribution(nBins: Int, data: List[Estimate], overrideBounds: Option[List[Double]] = None) {
  require(data.length > nBins)

  val Epsilon = 0.000001
  val sorted = data.sortBy(_.error)
  val min = sorted.head.error
  val max = sorted.last.error
  val binWidth = (max - min) / nBins + Epsilon
  val bounds = (1 to nBins).map { x => min + binWidth * x }.toList

  def histo(bounds: List[Double], data: List[Estimate]): List[(Double, List[Estimate])] =
    bounds match {
      case h :: Nil => List((h, data))
      case h :: t   => val (l,r) = data.partition( _.error < h) ; (h, l) :: histo(t,r)
    }

  val histogram = histo(overrideBounds.getOrElse(bounds), data)
}
