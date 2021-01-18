package simple.window.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import simple.window.watermark.agg.DistinctAggreFunction;
import simple.window.watermark.trigger.EventTimeTriggerOverload;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author:lcp
 * @CreateTime:2021-01-12 22:49:35
 * @Mark:滚动窗口水印demo 样例数据
 * User1&2021-01-05 20:42:23
 * User1&2021-01-05 20:44:23
 * User5&2021-01-05 20:42:45
 * User2&2021-01-05 20:45:23
 * User4&2021-01-05 20:45:54
 * User5&2021-01-05 20:46:32
 * User3&2021-01-05 20:45:44
 * User2&2021-01-05 20:47:32
 * User3&2021-01-05 20:47:44
 * User1&2021-01-05 20:46:20
 * User2&2021-01-05 20:47:53
 * User5&2021-01-05 20:48:03
 * <p>
 * <p>
 * 文章中的样例数据
 * User1&2021-01-05 20:44:23
 * User2&2021-01-05 20:45:23
 * User4&2021-01-05 20:45:54
 * User5&2021-01-05 20:46:32
 * User3&2021-01-05 20:45:44
 * User2&2021-01-05 20:47:32
 * User3&2021-01-05 20:47:44
 * User1&2021-01-05 20:46:20
 * User5&2021-01-05 20:44:54
 **/
public class TumblingWatermarkMain {
    private static final String SEPATOR = "&";
    private transient static final Time duration = Time.minutes(1L);
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(duration) {
            private static final long serialVersionUID = -7468901897687942904L;

            @Override
            public long extractTimestamp(String element) {
                long time = System.currentTimeMillis();
                try {
                    time = simpleDateFormat.parse(element.split(SEPATOR)[1]).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return time;
            }
        }).map((MapFunction<String, Tuple2<String, String>>) value -> {
            System.out.println();
            System.out.println("第一步：输入事件元素--->" + value);
            SimpleDateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            String user = value.split(SEPATOR)[0];
            String dateTime = value.split(SEPATOR)[1];
            return Tuple2.of(targetFormat.format(simpleDateFormat.parse(dateTime)), user);
        }).returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .trigger(EventTimeTriggerOverload.create())
                .aggregate(new DistinctAggreFunction()).print("计算结果值为--->");

        env.execute();
    }
}
