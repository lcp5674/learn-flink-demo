package simple.window.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-12-04 02:50:35
 * @Mark:CountEvictorDemo：保存指定数量的元素，多余的元素按照从前往后的顺序剔除
 *
 * 输入
 *1000,flink,1
 * 2000,flink,2
 * 2111,flink,3
 * 2222,flink,4
 * 3000,flink,5
 *
 * 输出：
 * (flink,7) --> 第一个窗口统计值
 **/
public class CountEvictorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);

        socketTextStream.map(t -> Tuple3.of(t.split(",")[0], t.split(",")[1], Integer.parseInt(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class,String.class, Integer.class))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Integer>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Integer> element) {
                        return Long.parseLong(element.f0);
                    }
                })
                .setParallelism(1)
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .evictor(CountEvictor.of(2, false))  //在window function调用前执行，窗口只保留2个元素
                .sum(2)
                .project(1,2)
                .print();


        env.execute();
    }
}
