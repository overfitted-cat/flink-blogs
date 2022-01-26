package simple.filtering.condition
import simple.filtering.model.Metric

class MetricValueGt(value: Double) extends Condition {

  override def evaluate(metric: Metric): Boolean = value < metric.value

}
