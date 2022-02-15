package simple.alerting.model

import java.sql.Timestamp

case class FiredRule(timestamp: Timestamp, value: Double, rule: RegionRule)
