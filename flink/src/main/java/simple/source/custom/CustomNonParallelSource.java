package simple.source.custom;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author:lcp
 * @CreateTime:2020-10-21 09:15:20
 * @Mark:
 **/
public class CustomNonParallelSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        List<String> list = new ArrayList<>();
        list.add("Spark");
        list.add("Hadoop");
        list.add("Flink");
        list.add("HBase");
        list.add("Es");
        SingleOutputStreamOperator<String> singleOutputStreamOperator = env.addSource(new MyWordSource(list)).returns(String.class);
        System.out.println("自定义非并行source对应的并行度:" + singleOutputStreamOperator.getParallelism());
        singleOutputStreamOperator.print();
        env.execute();
    }

    static class MyWordSource implements SourceFunction<String> {
        private List<String> words;
        private boolean isStop = false;

        public MyWordSource() {
        }

        public MyWordSource(List<String> words) {
            this.words = words;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            if (words != null && words.size() > 0) {
                while (!isStop) {
                    words.forEach(word -> ctx.collect(word));
                }
            }
        }

        @Override
        public void cancel() {
            isStop = true;
        }
    }
}
