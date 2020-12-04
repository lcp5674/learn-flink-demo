package simple.transformation;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author:lcp
 * @CreateTime:2020-10-14 08:50:56
 * @Mark:
 **/
public class FoldDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.flatMap((String words, Collector<Tuple2<String,Integer>> out)-> Arrays.stream(words.split(" ")).forEach(word-> out.collect(Tuple2.of(word,1)) ))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class,Integer.class))
                .keyBy(t->t.f0)
                .fold(Tuple2.of("", 0), new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> fold(Tuple2<String, Integer> tuple2, Tuple2<String, Integer> o) throws Exception {
                        //tuple2为中间存储的结果或初始化值
                        o.f1 = o.f1 + tuple2.f1;
                        return o;
                    }
                }).print();

        env.execute("FoldDemo");
    }
}
