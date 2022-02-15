package simple.alerting.source

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.streaming.api.functions.source.SourceFunction
import simple.alerting.model.RegionCount

import java.sql.Timestamp

class RegionCountGenerator extends SourceFunction[RegionCount] with LazyLogging {

  val COUNT_DATA = Seq(
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:00:00"), 3),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:00:00"), 23),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:00:00"), 3),
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:00:30"), 3),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:00:30"), 23),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:00:30"), 3),
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:01:00"), 3),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:01:00"), 23),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:01:00"), 3),
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:01:30"), 6),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:01:30"), 25),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:01:30"), 3),
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:02:00"), 7),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:02:00"), 26),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:02:00"), 3),
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:02:30"), 7),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:02:30"), 26),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:02:30"), 3),
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:03:00"), 7),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:03:00"), 26),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:03:00"), 3),
    RegionCount("BUILDING-1", "ROOM-1", Timestamp.valueOf("2022-01-31 00:03:30"), 8),
    RegionCount("BUILDING-1", "ROOM-2", Timestamp.valueOf("2022-01-31 00:03:30"), 25),
    RegionCount("BUILDING-2", "ROOM-55", Timestamp.valueOf("2022-01-31 00:03:30"), 3)
  )

  override def run(ctx: SourceFunction.SourceContext[RegionCount]): Unit =
    COUNT_DATA.foreach(ctx.collect)

  override def cancel(): Unit = {}
}
