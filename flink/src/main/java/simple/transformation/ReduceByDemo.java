package simple.transformation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author:lcp
 * @CreateTime:2020-10-14 08:40:14
 * @Mark:
 **/
public class ReduceByDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);
        socketTextStream.flatMap((String words, Collector<Tuple2<String,Integer>> out)-> Arrays.stream(words.split(" ")).forEach(word->out.collect(Tuple2.of(word,1))))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class,Integer.class))
                .keyBy(t->t.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        stringIntegerTuple2.f1 = stringIntegerTuple2.f1 + t1.f1;
                        return stringIntegerTuple2;
                    }
                })
                .print();


        /*socketTextStream.map(word -> Tuple2.of(word, 1))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(t -> t.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                        stringIntegerTuple2.f1 = stringIntegerTuple2.f1 + t1.f1;
                        return stringIntegerTuple2;
                    }
                })
                .print();
*/
        env.execute("ReduceByDemo");
    }
}
