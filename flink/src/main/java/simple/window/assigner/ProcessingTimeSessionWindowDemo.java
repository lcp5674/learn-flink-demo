package simple.window.assigner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 14:32:06
 * @Mark:分组的会话窗口，采用ProcessingTime来分配窗口
 **/
public class ProcessingTimeSessionWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map(t -> Tuple2.of(t.split(",")[0], Integer.valueOf(t.split(",")[1])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(t -> t.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .sum(1)
                .print();

        env.execute();
    }
}
