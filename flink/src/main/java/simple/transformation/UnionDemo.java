package simple.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:lcp
 * @CreateTime:2020-11-11 21:57:11
 * @Mark:
 **/
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> source2 = env.fromElements(11, 22, 33, 44, 55);
        DataStream<Integer> unionSource = source1.union(source2);
        unionSource.print();
        env.execute();
    }
}
