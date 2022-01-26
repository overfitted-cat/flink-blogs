package simple.filtering

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import simple.filtering.FilteringFunction.FILTER_STATE_DESCRIPTOR
import simple.filtering.condition.Condition
import simple.filtering.model.{Metric, MetricFilter}
import simple.filtering.source.{FilterGenerator, MetricGenerator}
import simple.filtering.condition.Condition._

import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`

class FilteringFunction extends RichCoFlatMapFunction[Metric, MetricFilter, Metric] with LazyLogging{

  /**
   * Applies filtering function to the metric input.
   */
  override def flatMap1(value: Metric, out: Collector[Metric]): Unit = {
    val filterState = getRuntimeContext.getMapState(FILTER_STATE_DESCRIPTOR)
    val conditions = filterState.values().toList
    if (conditions.isEmpty || conditions.forall(_(value)))
      out.collect(value)
  }

  /**
   * Applies state management for the filter input.
   */
  override def flatMap2(value: MetricFilter, out: Collector[Metric]): Unit = {
    if (value.isActive) upsertFilter(value) else removeFilter(value)
  }

  /**
   * Removes a filter from the state
   */
  private def removeFilter(metricFilter: MetricFilter): Unit = {
    val filterState = getRuntimeContext.getMapState(FILTER_STATE_DESCRIPTOR)
    if (filterState.contains(metricFilter.filterID)){
      filterState.remove(metricFilter.filterID)
      logger.info(s"removed filter from the state - $metricFilter")
    }else{
      logger.info(s"filter not in the state - $metricFilter")
    }
  }

  /**
   * Upserts a filter to the state.
   */
  private def upsertFilter(filter: MetricFilter): Unit = {
    val filterState = getRuntimeContext.getMapState(FILTER_STATE_DESCRIPTOR)
    filterState.put(filter.filterID, filter.filterDescription.toCondition)
    logger.info(s"upserted filter into state $filter")
  }
}

object FilteringFunction {
  val FILTER_STATE_DESCRIPTOR = new MapStateDescriptor[String, Condition](
    "filters", classOf[String], classOf[Condition])
}

object KeyedFilteringJob extends App with LazyLogging {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val metricsStream = env
    .addSource(MetricGenerator.getWithPause)
    .assignAscendingTimestamps(_.timestamp.getTime)
  val filterStream = env
    .addSource(FilterGenerator.getWithPause)
    .assignAscendingTimestamps(_.timestamp.getTime)
    .keyBy(filter => filter.regionID)

  metricsStream
    .keyBy(metric => metric.regionID)
    .connect(filterStream)
    .flatMap(new FilteringFunction())
    .print()

  env.execute()

}
