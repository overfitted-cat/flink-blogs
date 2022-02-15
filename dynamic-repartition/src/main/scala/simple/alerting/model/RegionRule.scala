package simple.alerting.model

import java.sql.Timestamp

case class RegionRule(
  ruleID: String,
  isActive: Boolean,
  timestamp: Timestamp,
  ruleDescription: RuleDescription
)
