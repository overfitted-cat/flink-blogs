package simple.filtering.model

import java.sql.Timestamp

case class Metric(sensorID: String,
                  regionID: String,
                  timestamp: Timestamp,
                  dataType: String,
                  value: Double)
