package simple.window.assigner;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 11:22:00
 * @Mark:不分组窗口/GlobalWindow窗口类型
 **/
public class CountWindowAllDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        //当窗口内的元素满足5条时，会触发
        AllWindowedStream<Integer, GlobalWindow> windowCount = socketTextStream.map(Integer::parseInt).countWindowAll(5);
        windowCount.sum(0).print();

        env.execute();
    }
}
