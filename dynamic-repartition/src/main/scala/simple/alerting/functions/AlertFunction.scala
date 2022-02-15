package simple.alerting.functions

import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.util.Collector
import simple.alerting.model.{FiredRule, KeyedEventAggregate, RegionRule}
import simple.alerting.rule.Rule._

class AlertFunction extends BroadcastProcessFunction[KeyedEventAggregate, RegionRule, FiredRule] {
  override def processElement(
    value: KeyedEventAggregate,
    ctx: BroadcastProcessFunction[KeyedEventAggregate, RegionRule, FiredRule]#ReadOnlyContext,
    out: Collector[FiredRule]
  ): Unit = {
    val state = ctx.getBroadcastState(RULE_DESCRIPTOR)
    Option(state.get(value.ruleID)).foreach { case (rule, check) =>
      val average = value.sum / value.count
      if (check(average))
        out.collect(FiredRule(value.timestamp, average, rule))
    }
  }

  override def processBroadcastElement(
    value: RegionRule,
    ctx: BroadcastProcessFunction[KeyedEventAggregate, RegionRule, FiredRule]#Context,
    out: Collector[FiredRule]
  ): Unit = {
    val state = ctx.getBroadcastState(RULE_DESCRIPTOR)
    if (value.isActive) {
      state.put(value.ruleID, (value, value.ruleDescription.asRuleCheck))
    } else {
      state.remove(value.ruleID)
    }
  }
}
