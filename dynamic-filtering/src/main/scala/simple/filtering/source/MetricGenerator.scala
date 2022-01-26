package simple.filtering.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.SourceFunction
import simple.filtering.model.Metric

import java.sql.Timestamp
import scala.util.Random

trait MetricGenerator extends SourceFunction[Metric] with LazyLogging

class MetricGeneratorWithPause extends SourceFunction[Metric] with LazyLogging{

  val TEMPERATURE = "TEMPERATURE"

  val beforePauseRegionATemperature = Seq(
    Metric("01", "A", Timestamp.valueOf("2022-01-10 10:35:00"), TEMPERATURE, 21.0),
    Metric("11", "B", Timestamp.valueOf("2022-01-10 10:35:00"), TEMPERATURE, 15.0),
    Metric("01", "A", Timestamp.valueOf("2022-01-10 10:40:00"), TEMPERATURE, 22.0),
    Metric("11", "B", Timestamp.valueOf("2022-01-10 10:40:00"), TEMPERATURE, 15.0),
    Metric("01", "A", Timestamp.valueOf("2022-01-10 10:45:00"), TEMPERATURE, 20.0),
    Metric("11", "B", Timestamp.valueOf("2022-01-10 10:45:00"), TEMPERATURE, 15.0),
    Metric("01", "A", Timestamp.valueOf("2022-01-10 10:50:00"), TEMPERATURE, 22.0))

  val afterPauseRegionATemperature = Seq(
    Metric("01", "A", Timestamp.valueOf("2022-01-10 11:30:00"), TEMPERATURE, 18.0),
    Metric("11", "B", Timestamp.valueOf("2022-01-10 11:30:00"), TEMPERATURE, 25.0),
    Metric("01", "A", Timestamp.valueOf("2022-01-10 11:35:00"), TEMPERATURE, 21.0),
    Metric("01", "A", Timestamp.valueOf("2022-01-10 11:40:00"), TEMPERATURE, 15.0))

  override def run(ctx: SourceFunction.SourceContext[Metric]): Unit = {
    logger.info("initial wait 1second")
    Thread.sleep(1000)
    beforePauseRegionATemperature.foreach{ metric =>
      ctx.collect(metric)
      Thread.sleep(200)
    }
    logger.info("metrics pause 5seconds")
    Thread.sleep(5000)
    logger.info("metrics continue after pause")
    afterPauseRegionATemperature.foreach{ metric =>
      ctx.collect(metric)
      Thread.sleep(200)
    }
  }

  override def cancel(): Unit = {}
}

class MetricGeneratorRandom extends MetricGenerator {
  val SLEEP_MS = 10
  val STOP_AFTER = 10000

  val sensors = Seq("01", "02", "03")
  val regions = Seq("A", "B", "C")
  val random = new Random()

  override def run(ctx: SourceFunction.SourceContext[Metric]): Unit = {
    1 to STOP_AFTER foreach { _ =>
      ctx.collect(getRandom)
      Thread.sleep(SLEEP_MS)
    }
  }

  def getRandom: Metric = {
    Metric(
      sensors(random.nextInt(sensors.length)),
      regions(random.nextInt(regions.length)),
      new Timestamp(System.currentTimeMillis()),
      "TEMPERATURE",
      random.nextInt(30))
  }

  override def cancel(): Unit = {}

}
object  MetricGenerator {

  def getRandom: MetricGeneratorRandom = new MetricGeneratorRandom()
  def getWithPause: MetricGeneratorWithPause = new MetricGeneratorWithPause()

}