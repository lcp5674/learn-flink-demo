package simple.transformation.aggregation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:lcp
 * @CreateTime:2020-10-15 08:58:46
 * @Mark:分组求组内最小值
 **/
public class MinMinByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map(word -> Tuple3.of(word.split(",")[0], word.split(",")[1], Integer.valueOf(word.split(",")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, Integer.class))
                .keyBy(t -> t.f0)
                .minBy(2,true)
                .print();

        env.execute("MinMinByDemo");
    }
}
