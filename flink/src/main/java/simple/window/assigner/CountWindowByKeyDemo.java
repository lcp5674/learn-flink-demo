package simple.window.assigner;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 11:31:10
 * @Mark:
 **/
public class CountWindowByKeyDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.map(t-> Tuple2.of(t.split(",")[0],Integer.valueOf(t.split(",")[1])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class,Integer.class))
                .keyBy(t->t.f0)
                .countWindowAll(5)
                .sum(1)
                .print();

        env.execute();
    }
}
