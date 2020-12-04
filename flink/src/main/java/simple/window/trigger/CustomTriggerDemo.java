package simple.window.trigger;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-12-03 18:13:43
 * @Mark:自定义触发器测试
 **/
public class CustomTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //这里一定要设置划分时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);

        socketTextStream.map(t -> Tuple2.of(t.split(",")[0] + "-" + t.split(",")[1], Integer.parseInt(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .setParallelism(1)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> element) {
                        return Long.parseLong(element.f0.split("-")[0]);
                    }
                })
                .keyBy(t -> t.f0.split("-")[1])
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//                .trigger(EventTimeTriggerDemo.create())
                .trigger(CustomCountEventTimeTrigger.of())
                .sum(1)
                .print();

        env.execute();
    }
}
