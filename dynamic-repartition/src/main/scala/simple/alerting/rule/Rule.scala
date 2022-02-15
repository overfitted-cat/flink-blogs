package simple.alerting.rule

import simple.alerting.model.RuleDescription

trait RuleCheck extends Serializable {

  def check(value: Double): Boolean
  def apply(value: Double): Boolean = check(value)

}
object Rule {

  private[this] case class Function2Rule(fun: Double => Boolean) extends RuleCheck {
    override def check(value: Double): Boolean = fun(value)
  }
  implicit def fromFunction(fun: Double => Boolean): RuleCheck = Function2Rule(fun)

  implicit class DescriptionToRule(ruleDescription: RuleDescription) {

    def asRuleCheck: RuleCheck = {
      ruleDescription.operator match {
        case ">=" => _ >= ruleDescription.threshold
        case "<=" => _ <= ruleDescription.threshold
        case ">"  => _ > ruleDescription.threshold
        case "<"  => _ < ruleDescription.threshold
      }
    }

  }

}
