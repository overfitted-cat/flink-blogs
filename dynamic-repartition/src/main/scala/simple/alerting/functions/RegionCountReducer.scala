package simple.alerting.functions

import org.apache.flink.api.common.functions.ReduceFunction
import simple.alerting.model.KeyedEventAggregate

class RegionCountReducer extends ReduceFunction[KeyedEventAggregate] {

  override def reduce(value1: KeyedEventAggregate, value2: KeyedEventAggregate): KeyedEventAggregate =
    value1.copy(timestamp = value2.timestamp, count = value1.count + 1, sum = value1.sum + value2.sum)
}
