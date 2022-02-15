package simple.alerting.model

case class RuleDescription(
  appliedOn: Map[String, String],
  threshold: Double,
  operator: String
)
