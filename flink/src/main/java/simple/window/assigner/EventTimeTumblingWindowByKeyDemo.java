package simple.window.assigner;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Int;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 15:47:14
 * @Mark:按照event_time分组的滚动窗口
 **/
public class EventTimeTumblingWindowByKeyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //设置event_time处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 10000);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> mapStream = socketTextStream.map(t -> Tuple3.of(Long.parseLong(t.split(",")[0]), t.split(",")[1], Integer.valueOf(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(Long.class, String.class, Integer.class));

        //需要等到所有的分区都符合时间条件时才会执行聚合操作
        mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                return element.f0;
            }
        }).keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3))) //设置滚动窗口长度为3s
                .sum(2)
                .project(1, 2)
                .print();

        env.execute();

    }
}
