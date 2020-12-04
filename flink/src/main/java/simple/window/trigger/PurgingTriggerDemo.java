package simple.window.trigger;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

/**
 * @Author:lcp
 * @CreateTime:2020-12-03 03:29:42
 * @Mark:其他的Trigger实现类都不会对窗口数据进行清除，这里对EventTimeTrigger做一个转换支持窗口清除 示例：使用滑动窗口，设置窗口大小为3s,每2s秒滑动一次，通过对kv的统计来验证是否会把窗口内的数据清除
 *输入：
 * 1000,flink,1
 * 2000,flink,2
 * 3000,flink,3
 * 4000,flink,4
 * 5000,flink,5
 * 6000,flink,6
 * 7000,flink,7
 * 8000,flink,8
 * 9000,flink,9
 * 10000,flink,10
 *
 * 输出结果：
 * 下次触发时间:2000
 * 下次触发时间:4000
 * 4> (1000-flink,3)
 * 下次触发时间:6000
 * 当等于窗口最大时间即开始触发:3999
 * 4> (3000-flink,3)
 * 下次触发时间:8000
 * 4> (4000-flink,15)
 * 下次触发时间:10000
 * 当等于窗口最大时间即开始触发:7999
 * 下次触发时间:12000
 * 4> (8000-flink,27)
 **/
public class PurgingTriggerDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);

        socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
//                System.out.println(Long.parseLong(element.split(",")[0]));
                return Long.parseLong(element.split(",")[0]);
            }
        })
                .map(t -> Tuple2.of(t.split(",")[0] + "-" + t.split(",")[1], Integer.parseInt(t.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(t -> t.f0.split("-")[1])
                .window(TumblingEventTimeWindows.of(Time.seconds(4))) //设置一个4s的窗口长度
                .trigger(PurgingTrigger.of(CustomEventTimeTrigger.of(Time.seconds(2)))) //每2s触发一次计算，相当于是多次嵌套触发，这里涉及到时间对齐,比如0~2,其中event_time=2000才会触发计算
                .sum(1)
                .print();

        env.execute();
    }
}
