package simple.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:lcp
 * @CreateTime:2020-10-14 08:27:33
 * @Mark:
 **/
public class KeyByDemo2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map(words->WordCount.of(words,1))
                .returns(WordCount.class)
                .keyBy(WordCount::getWord)
                .sum("count")
                .print();

        env.execute("KeyByDemo2");
    }
}
