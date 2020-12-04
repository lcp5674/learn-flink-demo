package simple.window.assigner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * @Author:lcp
 * @CreateTime:2020-10-20 14:42:24
 * @Mark:按照event_time划分滚动窗口（不分组）
 **/
public class EventTimeTumblingWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //设置EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        //提取event_time，转换为时间戳格式(单位：毫秒)
        //输入格式:1603036800000,1
        //1603036802899,1
        //1603036802999,1
        //1603036803999,1
        //1603036804499,1
        //1603036804999,1
        //1603036805999,1

        //这里是对source产生的DataStream提取出事件时间
        SingleOutputStreamOperator<String> outputStreamOperator = socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {

            @Override
            public long extractTimestamp(String element) {
                System.out.println(Long.parseLong(element.split(",")[0]));
                return Long.parseLong(element.split(",")[0]);
            }
        });

        outputStreamOperator.map(t -> Tuple2.of(t.split(",")[0], Integer.valueOf(t.split(",")[1])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                .sum(1)
                .print();

        env.execute();
    }
}
