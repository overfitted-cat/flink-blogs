package simple.alerting.model

import java.sql.Timestamp

case class KeyedEventAggregate(key: String, ruleID: String, timestamp: Timestamp, count: Int, sum: Double)
