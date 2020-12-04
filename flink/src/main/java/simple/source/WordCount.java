package simple.source;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author:lcp
 * @CreateTime:2020-10-12 08:40:24
 * @Mark:
 **/
public class WordCount {

    public static void main(String[] args) throws Exception {
        //并行度和系统核数一致
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //非并行的source,只有1个并行度
        DataStreamSource<String> lines = env.socketTextStream(args[0], Integer.parseInt(args[1]));
        int parallelism = lines.getParallelism();
        System.out.println("socketTextStream并行度："+parallelism);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> Arrays.stream(line.split(" ")).forEach(word -> out.collect(Tuple2.of(word, 1)))).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(t -> t.f0).sum(1);
        summed.print();

        env.execute("WordCount");
    }
}
