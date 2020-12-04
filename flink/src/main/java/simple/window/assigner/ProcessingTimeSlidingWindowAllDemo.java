package simple.window.assigner;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 14:10:18
 * @Mark:不分组的滑动窗口统计
 **/
public class ProcessingTimeSlidingWindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map(Integer::parseInt)
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .sum(0)
                .print();

        env.execute();
    }
}
