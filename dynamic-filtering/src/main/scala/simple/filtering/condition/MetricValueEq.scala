package simple.filtering.condition
import simple.filtering.model.Metric

class MetricValueEq(value: Double, tol:Double = 1.0e-3) extends Condition {

  override def evaluate(metric: Metric): Boolean = Math.abs(value - metric.value) <= tol
}
