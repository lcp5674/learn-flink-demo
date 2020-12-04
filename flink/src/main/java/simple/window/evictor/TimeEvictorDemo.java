package simple.window.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-12-04 20:14:13
 * @Mark: TimeEvictor作用演示:对于给定的窗口,提供一个以毫秒为单位间隔的参数interval，找到最大的时间戳max_ts，然后删除所有时间戳小于max_ts-interval
 *输入：
 * 1000,flink,1
 * 1500,flink,2
 * 2000,flink,3
 * 2500,flink,4
 * 3000,flink,5
 *
 * 输出
 *  (flink,7) --->当前窗口最大event_time为3s,即剔除小于(3-1)的元素，只保留了(2000,flink,3)和（2500,flink,4）
 **/
public class TimeEvictorDemo {
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
                .evictor(TimeEvictor.of(Time.seconds(1), false))  //在window function调用前执行，删除窗口中event_time < max(event_time)-1s 的元素
                .sum(2)
                .project(1,2)
                .print();


        env.execute();
    }
}
