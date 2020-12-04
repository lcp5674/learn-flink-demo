package simple.window.function;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-12-04 07:21:41
 * @Mark:统计每个窗口所有key对应的value总和
 **/
public class FoldFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);
        socketTextStream.map(t-> Tuple3.of(t.split(",")[0],t.split(",")[1],Integer.parseInt(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class,String.class,Integer.class))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Integer>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Integer> element) {
                        return Long.parseLong(element.f0);
                    }
                })
                .setParallelism(1)
                .keyBy(t->t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .fold(0,new CustomFoldFunction()).print();

        env.execute();

    }
}

class CustomFoldFunction implements FoldFunction<Tuple3<String,String,Integer>,Integer>{

    @Override
    public Integer fold(Integer accumulator, Tuple3<String, String, Integer> value) throws Exception {
        accumulator = accumulator+value.f2;
        return accumulator;
    }
}
