package simple.transformation;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author:lcp
 * @CreateTime:2020-10-15 21:42:08
 * @Mark:
 **/
public class SplitSelectDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> dataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);

        dataStreamSource.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> tags = new ArrayList<String>();
                if (value % 2 == 0) {
                    tags.add("EVEN");
                } else {
                    tags.add("ODD");
                }
                return tags;
            }
        }).select("ODD")
                .print();

        env.execute("SplitSelectDemo");
    }
}
