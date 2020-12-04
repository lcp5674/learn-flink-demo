package simple.window.function;

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
 * @CreateTime:2020-12-04 07:34:56
 * @Mark:使用ProcessWindowFunction来统计每个窗口内每个key对应的value之和
 **/
public class ProcessWindowFunctionDemo {
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
                })
                .setParallelism(1)
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .process(new MyProcessWindowFunction())
                .print();

        env.execute();
    }
}

class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, String, Integer>, String, String, TimeWindow> {


    @Override
    public void process(String s, Context context, Iterable<Tuple3<String, String, Integer>> elements, Collector<String> out) throws Exception {
        long count = 0;
        for (Tuple3<String, String, Integer> element : elements) {
            count += element.f2;
        }
        out.collect("Window:" + context.window() + "Key:" + s + " Count:" + count);
    }
}