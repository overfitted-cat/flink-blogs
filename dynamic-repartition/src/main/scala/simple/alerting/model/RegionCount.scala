package simple.alerting.model

import java.sql.Timestamp

case class RegionCount(spaceID: String, regionID: String, timestamp: Timestamp, count: Double)
