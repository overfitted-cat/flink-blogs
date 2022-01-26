package simple.filtering.condition

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{DateTime, DateTimeComparator}
import simple.filtering.model.Metric

import java.sql.Time

class MetricTimeAfter(time: Time) extends Condition with LazyLogging{
  val comparator = DateTimeComparator.getTimeOnlyInstance

  override def evaluate(metric: Metric): Boolean = {
    val refTime = new DateTime(time.getTime)
    val metricTime = new DateTime(metric.timestamp)
    comparator.compare(refTime, metricTime) < 0

  }

}
