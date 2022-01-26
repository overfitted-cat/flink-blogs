package simple.filtering.condition

import simple.filtering.model.{FilterDescription, Metric}

import java.sql.Time

abstract class Condition extends Serializable{

  def evaluate(metric: Metric): Boolean
  def apply(metric: Metric): Boolean = evaluate(metric)

  def &&(condition: Condition): Condition = new AndCondition(this, condition)
  def ||(condition: Condition): Condition = new OrCondition(this, condition)
  def unary_!():Condition = new NotCondition(this)

}

sealed class AndCondition(condition_1: Condition, condition_2: Condition) extends Condition {
  override def evaluate(metric: Metric): Boolean = condition_1(metric) && condition_2(metric)
}
sealed class OrCondition(condition_1: Condition, condition_2: Condition) extends Condition {
  override def evaluate(metric: Metric): Boolean = condition_1(metric) || condition_2(metric)
}
sealed class NotCondition(condition: Condition) extends Condition {
  override def evaluate(metric: Metric): Boolean = !condition(metric)
}

object Condition {

  private[this] class ConditionImpl(func: Metric => Boolean) extends Condition{
    override def evaluate(metric: Metric): Boolean = func(metric)
  }

  implicit def funcToCondition(func: Metric => Boolean): Condition = new ConditionImpl(func)

  implicit class ToCondition(filterDescription: FilterDescription){
    def toCondition:Condition = {
      filterDescription.filterType match {
        case "VALUE" => filterDescription.operator match {
          case "==" => new MetricValueEq(filterDescription.value.toDouble)
          case ">" => new MetricValueGt(filterDescription.value.toDouble)
          case "<" => new MetricValueLt(filterDescription.value.toDouble)
          case "<=" => new MetricValueLt(filterDescription.value.toDouble) || new MetricValueEq(filterDescription.value.toDouble)
          case ">=" => new MetricValueGt(filterDescription.value.toDouble) || new MetricValueEq(filterDescription.value.toDouble)
          case "!=" => ! new MetricValueEq(filterDescription.value.toDouble)
        }
        case "TIMESTAMP" => filterDescription.operator match {
          case "AFTER" => new MetricTimeAfter(Time.valueOf(filterDescription.value))
        }
      }
    }
  }

}