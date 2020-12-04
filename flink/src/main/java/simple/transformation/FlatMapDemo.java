package simple.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author:lcp
 * @CreateTime:2020-10-13 09:28:44
 * @Mark:
 **/
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> flatMap = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                Arrays.stream(s.split(" ")).forEach(word -> collector.collect(word.toUpperCase()));
            }
        });

        flatMap.print();

        env.execute("FlatMapDemo");
    }
}
