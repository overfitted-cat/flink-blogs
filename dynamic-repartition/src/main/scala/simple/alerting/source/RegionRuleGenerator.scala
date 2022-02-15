package simple.alerting.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.joda.time.DateTime
import simple.alerting.model.{RegionRule, RuleDescription}

import java.sql.Timestamp

class RegionRuleGenerator extends SourceFunction[RegionRule] {
  val RULES_DATA = Seq(
    RegionRule(
      "RULE-1",
      true,
      new Timestamp(DateTime.now().getMillis),
      RuleDescription(Map("regionID" -> "ROOM-1", "spaceID" -> "BUILDING-1"), 5.0, ">=")
    ),
    RegionRule(
      "RULE-2",
      true,
      new Timestamp(DateTime.now().getMillis),
      RuleDescription(Map("spaceID" -> "BUILDING-1"), 10.0, ">=")
    ),
    RegionRule(
      "RULE-3",
      true,
      new Timestamp(DateTime.now().getMillis),
      RuleDescription(Map("spaceID" -> "BUILDING-2", "regionID" -> "ROOM-55"), 2.0, ">=")
    )
  )

  override def run(ctx: SourceFunction.SourceContext[RegionRule]): Unit =
    RULES_DATA.foreach(ctx.collect)

  override def cancel(): Unit = {}
}
