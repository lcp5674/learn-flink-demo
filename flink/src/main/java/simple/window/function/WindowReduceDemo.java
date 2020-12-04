package simple.window.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-10-22 08:43:27
 * @Mark:先keyBy，然后划分窗口，再对窗口中的数据聚合
 **/
public class WindowReduceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        //这里是先进行map操作，后面在操作提取水印的时候属于多并行
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> streamOperator = socketTextStream.map(t -> Tuple3.of(t.split(",")[0], t.split(",")[1], Integer.parseInt(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, Integer.class));

        streamOperator.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Integer>>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Integer> element) {
                return Long.parseLong(element.f0);
            }
        }).map(t -> Tuple2.of(t.f1, t.f2))
        .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
        .keyBy(t -> t.f0)
        .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                value1.f1 = value1.f1 + value2.f1;
                return value1;
            }
        })
        .print();

        env.execute();
    }
}
