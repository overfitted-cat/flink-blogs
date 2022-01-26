package simple.filtering.condition
import simple.filtering.model.Metric

class MetricValueLt(value: Double) extends Condition {

  override def evaluate(metric: Metric): Boolean = metric.value < value

}
