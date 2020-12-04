package simple.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;

/**
 * @Author:lcp
 * @CreateTime:2020-10-12 09:44:06
 * @Mark:非并行source,即只有一个并行，subtask
 **/
public class NonParallelSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //如果直接实现了SourceFunction，就是非并行的source
        DataStreamSource<String> socketSource = env.addSource(new SocketTextStreamFunction("localhost", 8888, "\n", 3));

        DataStreamSource<Integer> fromSource = env.fromElements(1, 2, 3, 4);
        int parallelism = fromSource.getParallelism();
        System.out.println("fromElements并行度:" + parallelism);

    }
}
