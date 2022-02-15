package simple.alerting.functions

import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector
import simple.alerting.model.{KeyedEvent, RegionCount, RegionRule}
import simple.alerting.rule.Rule._

import scala.collection.JavaConverters._

class RegionDynamicKey extends BroadcastProcessFunction[RegionCount, RegionRule, KeyedEvent] {
  val SPACE_ID = "spaceID"
  val REGION_ID = "regionID"

  override def processElement(
    value: RegionCount,
    ctx: BroadcastProcessFunction[RegionCount, RegionRule, KeyedEvent]#ReadOnlyContext,
    out: Collector[KeyedEvent]
  ): Unit = {
    // create a scala map of rules from the broadcast state
    val rules = {
      for {
        el <- ctx.getBroadcastState(RULE_DESCRIPTOR).immutableEntries().asScala
      } yield (el.getKey -> el.getValue)
    }.toMap

    // get rules that matches current value's space and region
    // the rule can be applied to all regions inside space, or at particular space - region
    // in practice, this is the place where we can apply much complex matching logic
    // create keyed event out of current value and rule with matching key
    rules
      .filter { case (_, (rule, _)) =>
        value.regionID == rule.ruleDescription.appliedOn.getOrElse(
          REGION_ID,
          value.regionID
        ) && value.spaceID == rule.ruleDescription.appliedOn.getOrElse(SPACE_ID, value.spaceID)
      }
      .foreach { case (ruleID, (rule, _)) =>
        val key = List(
          ruleID,
          rule.ruleDescription.appliedOn.getOrElse(SPACE_ID, ""),
          rule.ruleDescription.appliedOn.getOrElse(REGION_ID, "")
        ).mkString("--") // Here we can apply something better for key generation
        out.collect(KeyedEvent(value, key, ruleID))
      }
  }

  override def processBroadcastElement(
    value: RegionRule,
    ctx: BroadcastProcessFunction[RegionCount, RegionRule, KeyedEvent]#Context,
    out: Collector[KeyedEvent]
  ): Unit = {
    val state = ctx.getBroadcastState(RULE_DESCRIPTOR)
    if (value.isActive) {
      state.put(value.ruleID, (value, value.ruleDescription.asRuleCheck))
    } else {
      state.remove(value.ruleID)
    }
  }
}
