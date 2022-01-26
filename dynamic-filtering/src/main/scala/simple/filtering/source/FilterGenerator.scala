package simple.filtering.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.SourceFunction
import simple.filtering.model.{FilterDescription, MetricFilter}

import java.sql.Timestamp

trait FilterGenerator extends SourceFunction[MetricFilter] with LazyLogging

class FilterGeneratorWithPause extends FilterGenerator{

  val filtersABefore = Seq(
    MetricFilter(
      "AFTER_TIME", true, new Timestamp(System.currentTimeMillis()), "A", FilterDescription("TIMESTAMP", "AFTER", "10:40:00")),
    MetricFilter(
      "GT_VALUE", true, new Timestamp(System.currentTimeMillis()), "A", FilterDescription("VALUE", ">", "20")
    )
  )
  val bFilter = MetricFilter(
    "GT_VALUE", true, new Timestamp(System.currentTimeMillis()), "B", FilterDescription("VALUE", ">=", "20"))


  val filtersAAfter = Seq(
    MetricFilter(
      "GT_VALUE", false, new Timestamp(System.currentTimeMillis()), "A", FilterDescription("VALUE", ">", "20")))



  override def run(ctx: SourceFunction.SourceContext[MetricFilter]): Unit = {
    ctx.collect(bFilter)
    filtersABefore.foreach{ctx.collect}
    logger.info("initialized filters, waiting 3seconds")
    Thread.sleep(3000)
    logger.info("updating A filter rule")
    filtersAAfter.foreach(ctx.collect)
  }

  override def cancel(): Unit = {}

}

class FilterGeneratorSimple extends FilterGenerator {

  val filters = Seq(
    MetricFilter("VALUE_GREATER", true, new Timestamp(System.currentTimeMillis()), "A", FilterDescription("VALUE", ">=", "20.0")),
    MetricFilter("VALUE_GREATER", true, new Timestamp(System.currentTimeMillis()), "B", FilterDescription("VALUE", ">=", "20.0")),
    MetricFilter("VALUE_GREATER", true, new Timestamp(System.currentTimeMillis()), "C", FilterDescription("VALUE", ">=", "20.0"))
  )

  override def run(ctx: SourceFunction.SourceContext[MetricFilter]): Unit = {
    filters.foreach(ctx.collect)
  }

  override def cancel(): Unit = {}

}

object FilterGenerator {

  def getWithPause: FilterGeneratorWithPause = new FilterGeneratorWithPause()
  def getSimple: FilterGeneratorSimple = new FilterGeneratorSimple()

}