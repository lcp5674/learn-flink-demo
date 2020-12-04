package simple.window.function;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author:lcp
 * @CreateTime:2020-12-04 08:16:07
 * @Mark: ProcessWindowFunction和ReduceFunction结合使用
 * 获取每个窗口每个key最小的value
 **/
public class ProcessWindowAndIncrementFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);

        socketTextStream.map(t -> Tuple3.of(t.split(",")[0], t.split(",")[1], Integer.parseInt(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, Integer.class))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Integer>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Integer> element) {
                        return Long.parseLong(element.f0);
                    }
                }).setParallelism(1)
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .reduce(new MyReduceFunction(), new CustomProcessWindowFunction())
                .print();

        env.execute();

    }
}

/**
 * 比对key对应的value值
 */
class MyReduceFunction implements ReduceFunction<Tuple3<String, String, Integer>> {
    @Override
    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
        return value1.f2 < value2.f2 ? value1 : value2;
    }
}

/**
 * 直接输出
 */
class CustomProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Tuple3<String, String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {
        Tuple3<String, String, Integer> res = elements.iterator().next();
        out.collect(Tuple2.of(res.f1, res.f2));
    }
}