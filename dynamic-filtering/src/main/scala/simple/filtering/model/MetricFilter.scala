package simple.filtering.model

import java.sql.Timestamp

case class MetricFilter(filterID: String,
                        isActive: Boolean,
                        timestamp: Timestamp,
                        regionID: String,
                        filterDescription: FilterDescription)
