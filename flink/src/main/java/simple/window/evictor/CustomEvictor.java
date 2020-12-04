package simple.window.evictor;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

/**
 * @Author:lcp
 * @CreateTime:2020-12-04 20:35:56
 * @Mark:自定义Evictor 输入:
 * 1000,flink,1
 * 1500,flink,2
 * 2000,flink,3
 * 2500,flink,4
 * 2600,flink,5
 * 3000,flink,6
 * <p>
 * 输出：
 * (flink,14)
 **/
public class CustomEvictor {
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
                .evictor(MyEvictor.of(false))  //在window function调用前执行，窗口只保留2个元素
                .sum(2)
                .project(1, 2)
                .print();


        env.execute();
    }
}

/**
 * 剔除偶数对应的元素
 */
class MyEvictor implements Evictor<Tuple3<String, String, Integer>, TimeWindow> {
    private final boolean doEvictAfter;

    private MyEvictor() {
        this.doEvictAfter = false;
    }

    private MyEvictor(boolean doEvictAfter) {
        this.doEvictAfter = doEvictAfter;
    }

    @Override
    public void evictBefore(Iterable<TimestampedValue<Tuple3<String, String, Integer>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        System.out.println("window function执行前调用");
        if (!doEvictAfter) {
            evict(elements, size, evictorContext);
        }
    }

    @Override
    public void evictAfter(Iterable<TimestampedValue<Tuple3<String, String, Integer>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
        System.out.println("window function执行后调用");
        if (doEvictAfter) {
            evict(elements, size, evictorContext);
        }
    }

    /**
     * 过滤偶数元素
     *
     * @param elements
     * @param size
     * @param ctx
     */
    private void evict(Iterable<TimestampedValue<Tuple3<String, String, Integer>>> elements, int size, EvictorContext ctx) {
        Iterator<TimestampedValue<Tuple3<String, String, Integer>>> valueIterator = elements.iterator();
        while (valueIterator.hasNext()) {
            Tuple3<String, String, Integer> value = valueIterator.next().getValue();
            if (value.f2 % 2 == 0) {
                System.out.println("当前元素是偶数,被删除:" + value);
                valueIterator.remove();
            }
        }
    }


    public static MyEvictor of(boolean doEvictAfter) {
        return new MyEvictor(doEvictAfter);
    }

    public static MyEvictor of() {
        return new MyEvictor(false);
    }

}
