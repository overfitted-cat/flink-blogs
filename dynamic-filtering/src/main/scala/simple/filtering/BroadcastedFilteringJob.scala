package simple.filtering

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import simple.filtering.source.{FilterGenerator, MetricGenerator}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import simple.filtering.BroadcastFilter.FILTER_STATE_DESCRIPTOR
import simple.filtering.condition.Condition
import simple.filtering.model.{Metric, MetricFilter}
import simple.filtering.condition.Condition._

/**
 * Models a join operator that joins broadcast filter stream and metrics stream.
 */
class BroadcastFilter extends BroadcastProcessFunction[Metric, MetricFilter, Metric] with LazyLogging{

  /**
   * Gets current filters for a metric key and applies them on metric
   */
  override def processElement(value: Metric,
                              ctx: BroadcastProcessFunction[Metric, MetricFilter, Metric]#ReadOnlyContext,
                              out: Collector[Metric]): Unit = {
    val filters = Option(ctx.getBroadcastState(FILTER_STATE_DESCRIPTOR).get(value.regionID))
    filters match {
      case Some(filtersMap) => if(filtersMap.values.forall(_(value))) out.collect(value)
      case None => out.collect(value)
    }
  }

  /**
   * Updates broadcast filter state. Removes filter if it is no longer active.
   */
  override def processBroadcastElement(value: MetricFilter,
                                       ctx: BroadcastProcessFunction[Metric, MetricFilter, Metric]#Context,
                                       out: Collector[Metric]): Unit = {
    if(value.isActive) upsertFilter(value, ctx) else removeFilter(value, ctx)
  }

  /**
   * Removes inactive filter from the filter broadcast state.
   */
  private def removeFilter(metricFilter: MetricFilter,
                           ctx: BroadcastProcessFunction[Metric, MetricFilter, Metric]#Context): Unit = {
    val filterState = ctx.getBroadcastState(FILTER_STATE_DESCRIPTOR)
    val conditions = Option(filterState.get(metricFilter.regionID))
    conditions match {
      case Some(filterMap) =>
        val newFilterMap = filterMap - metricFilter.filterID
        if (newFilterMap.isEmpty){
          filterState.remove(metricFilter.regionID)
        } else{
          filterState.put(metricFilter.regionID, newFilterMap)
        }
        logger.info(s"removed filter from the state - $metricFilter")
      case None => logger.info(s"filter is not in the state - $metricFilter")
    }
  }

  /**
   * Upserts a filter into broadcast state.
   */
  private def upsertFilter(filter: MetricFilter,
                           ctx: BroadcastProcessFunction[Metric, MetricFilter, Metric]#Context): Unit = {
    val filterState = ctx.getBroadcastState(FILTER_STATE_DESCRIPTOR)
    val filterMap = Option(filterState.get(filter.regionID)).getOrElse(Map())
    val newFilterMap = filterMap + (filter.filterID -> filter.filterDescription.toCondition)
    filterState.put(filter.regionID, newFilterMap)
    logger.info(s"upserted filter into state $filter")
  }
}
object BroadcastFilter{
  val FILTER_STATE_DESCRIPTOR = new MapStateDescriptor[String, Map[String, Condition]](
    "filter-rules", classOf[String], classOf[Map[String, Condition]])
}

object BroadcastedFilteringJob extends App with LazyLogging {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val metricsStream = env
    .addSource(MetricGenerator.getWithPause)
    .assignAscendingTimestamps(_.timestamp.getTime)
  val filterStream = env
    .addSource(FilterGenerator.getWithPause)
    .assignAscendingTimestamps(_.timestamp.getTime)
    .broadcast(BroadcastFilter.FILTER_STATE_DESCRIPTOR)

  metricsStream
    .connect(filterStream)
    .process(new BroadcastFilter())
    .print()

  env.execute()
}
