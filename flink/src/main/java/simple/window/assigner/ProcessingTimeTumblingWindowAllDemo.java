package simple.window.assigner;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 13:37:20
 * @Mark:使用ProcessTime划分窗口，使用滑动窗口且不分组
 **/
public class ProcessingTimeTumblingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map(Integer::parseInt)
                .timeWindowAll(Time.seconds(5))
                .sum(0)
                .print();

        //和上面的效果一样
        /*socketTextStream.map(Integer::parseInt)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(0)
                .print();*/

        env.execute();
    }
}
