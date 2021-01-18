package simple.window.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import simple.window.watermark.agg.DistinctAggreFunctionStrategy;
import simple.window.watermark.assigner.CustomSerializableTimestampAssignerWithNoSource;
import simple.window.watermark.trigger.EventTimeTriggerOverload;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

import static simple.window.watermark.TumblingWatermarkCustomMain.getLongStringTuple2;

/**
 * @Author:lcp
 * @CreateTime:2021-01-14 13:59:47
 * @Mark: * 文章中的样例数据
 * * User1&2021-01-05 20:44:23
 * * User2&2021-01-05 20:45:23
 * * User4&2021-01-05 20:45:54
 * * User5&2021-01-05 20:46:32
 * * User3&2021-01-05 20:45:44
 * * User2&2021-01-05 20:47:32
 * * User3&2021-01-05 20:47:44
 * * User1&2021-01-05 20:46:20
 * * User5&2021-01-05 20:44:54
 **/
public class TumblingWatermarkStrategyMain {
    private static final String SEPATOR = "&";
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat targetFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map((MapFunction<String, Tuple2<Long, String>>) value -> getLongStringTuple2(value, simpleDateFormat, SEPATOR))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(Long.class, String.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, String>>forBoundedOutOfOrderness(Duration.ofMinutes(1L)).withTimestampAssigner(new CustomSerializableTimestampAssignerWithNoSource()))
                .keyBy(t -> targetFormat.format(new Date(t.f0)))
                .window(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .trigger(EventTimeTriggerOverload.create())
                .aggregate(new DistinctAggreFunctionStrategy()).print("计算结果值为--->");

        env.execute();
    }
}
