package simple.window.function;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
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
 * @CreateTime:2020-12-04 04:44:27
 * @Mark:AggregateFunction示例 统计窗口内元素值平均数
 **/
public class AggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);

        socketTextStream.map(new RichMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                System.out.println("当前事件所在分区:" + getRuntimeContext().getIndexOfThisSubtask());
                return Tuple3.of(value.split(",")[0], value.split(",")[1], Integer.parseInt(value.split(",")[2]));
            }
        })
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, Integer.class))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Integer>>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Integer> element) {
                        return Long.parseLong(element.f0);
                    }
                }).setParallelism(1) //这里设置并行度为1是为了验证AggregateFunction功能,否则会出现多个channle无法对齐watermark造成应该触发窗口的时候发现未触发的现象
                .keyBy(t -> t.f1) //每个分组都满足条件时才会触发计算
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .aggregate(new MyAggregateFunction())
                .print();


        env.execute();
    }
}


/***
 * AggregateFunction实现类
 * IN:Tuple3<String, String, Integer> ,即<时间戳,key,value></>
 * ACC:Tuple2<Integer, Integer>,即<元素个数，元素值总和></>
 * OUT:Double,输出结果
 */
class MyAggregateFunction implements AggregateFunction<Tuple3<String, String, Integer>, Tuple2<Integer, Integer>, Double> {
    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return Tuple2.of(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(Tuple3<String, String, Integer> value, Tuple2<Integer, Integer> accumulator) {
        accumulator.f0 = accumulator.f0 + 1;
        accumulator.f1 = accumulator.f1 + value.f2;
        System.out.println("新增元素,当前中间状态结果:" + accumulator + " 当前事件元素：" + value);
        return accumulator;
    }

    @Override
    public Double getResult(Tuple2<Integer, Integer> accumulator) {
        System.out.println("调用结果值..分子为：" + accumulator.f1 + " 分母为:" + accumulator.f0);
        return Double.valueOf(accumulator.f1 / accumulator.f0);
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
        System.out.println("合并值:" + a);
        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
    }
}