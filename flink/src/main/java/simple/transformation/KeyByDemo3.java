package simple.transformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:lcp
 * @CreateTime:2020-10-13 22:12:05
 * @Mark:DataStream-->KeyedStream 即把相同的key放到同一个TaskManager中
 **/
public class KeyByDemo3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map(word -> Tuple3.of(word.split(" ")[0], word.split(" ")[1], Double.valueOf(word.split(" ")[2])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, Double.class))
                .keyBy(new KeySelector<Tuple3<String, String, Double>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Double> value) throws Exception {
                        return value.f0 + value.f1;
                    }
                })
//                .keyBy(t->t.f0+t.f1 ) //这里不支持 In many cases lambda methods don't provide enough information for automatic type extraction
                .sum(2)
                .print();

        env.execute("KeyByDemo");
    }
}
