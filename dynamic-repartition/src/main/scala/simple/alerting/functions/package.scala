package simple.alerting

import org.apache.flink.api.common.state.MapStateDescriptor
import simple.alerting.model.RegionRule
import simple.alerting.rule.RuleCheck

package object functions {
  val RULE_DESCRIPTOR = new MapStateDescriptor[String, (RegionRule, RuleCheck)](
    "region-rules",
    classOf[String],
    classOf[(RegionRule, RuleCheck)]
  )
}
