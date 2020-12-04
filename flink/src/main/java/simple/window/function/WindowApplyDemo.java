package simple.window.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author:lcp
 * @CreateTime:2020-10-22 08:16:25
 * @Mark:将数据分组后，然后将数据放入到windowState中，窗口触发后，将WindowState中的数据，遍历处理累加
 **/
public class WindowApplyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.设置event_time处理机制
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);


        socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[0]);
            }
        }).map(t -> Tuple2.of(t.split(",")[1], Integer.parseInt(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int count = 0;
                        for (Tuple2<String, Integer> tuple2 : input) {
                            count += tuple2.f1;
                        }
                        out.collect(Tuple2.of(s, count));
                    }
                }).print();

        env.execute();
    }
}
