package simple.window.assigner;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 14:42:37
 * @Mark:按照event_time不分组滑动统计
 **/
public class EventTimeSlidingWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[0]);
            }
        }).map(t-> Tuple2.of(t.split(",")[0],Integer.parseInt(t.split(",")[1])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class,Integer.class))
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5))) //设置滑动窗口，窗口长度为10s,每5s滑动一次
                .sum(1)
                .print();

        env.execute();
    }
}
