package simple.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.LongValueSequenceIterator;
import org.apache.flink.util.NumberSequenceIterator;

/**
 * @Author:lcp
 * @CreateTime:2020-10-12 09:45:33
 * @Mark:并行的source
 **/
public class ParllelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setMaxParallelism(4);

        DataStreamSource<Long> longs = env.fromParallelCollection(new NumberSequenceIterator(1L, 100L), Long.class);
        int parallelism1 = longs.getParallelism();
        longs.print();
        System.out.println("fromParallelCollection并行度:" + parallelism1);

        DataStreamSource<Long> nums = env.generateSequence(1, 1000);

        int parallelism = nums.getParallelism();


        nums.print();
        System.out.println("generateSequence并行度:" + parallelism);
        env.execute("ParllelSource");
    }
}
