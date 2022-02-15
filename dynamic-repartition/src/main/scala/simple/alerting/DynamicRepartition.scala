package simple.alerting

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import simple.alerting.model.KeyedEventAggregate
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import simple.alerting.functions.{AlertFunction, RULE_DESCRIPTOR, RegionCountReducer, RegionDynamicKey}
import simple.alerting.source.{RegionCountGenerator, RegionRuleGenerator}

object DynamicRepartition extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(3)
  val countsStream = env
    .addSource(new RegionCountGenerator())
    .assignAscendingTimestamps(_.timestamp.getTime)
  val rulesStream = env
    .addSource(new RegionRuleGenerator())
    .assignAscendingTimestamps(_.timestamp.getTime)
    .broadcast(RULE_DESCRIPTOR)

  countsStream
    .connect(rulesStream)
    .process(new RegionDynamicKey())
    .map(event => KeyedEventAggregate(event.key, event.ruleID, event.event.timestamp, 1, event.event.count))
    .keyBy(event => event.key)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .reduce(new RegionCountReducer())
    .connect(rulesStream)
    .process(new AlertFunction())
    .print()

  env.execute()
}
