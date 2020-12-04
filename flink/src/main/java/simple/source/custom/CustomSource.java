package simple.source.custom;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author:lcp
 * @CreateTime:2020-10-13 08:16:04
 * @Mark:自定义source
 **/
public class CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<String> words = new ArrayList<>();
        words.add("spark");
        words.add("hadoop");
        words.add("flink");
//        DataStreamSource<String> wordsSource = env.addSource(new MyWordSource(words));
        SingleOutputStreamOperator singleOutputStreamOperator = env.addSource(new MyWordParallelSource(words)).returns(String.class);

//        int parallelism = wordsSource.getParallelism();
        System.out.println("自定义并行source对应parallelism:" + singleOutputStreamOperator.getParallelism());
        singleOutputStreamOperator.print();
        env.execute("CustomSource");
    }

    static class MyWordParallelSource extends RichParallelSourceFunction {
        private boolean isStop = false;
        private List<String> words;

        public MyWordParallelSource() {
        }

        /**
         * 通常在该方法做一些初始化的工作，如创建数据库连接等
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        /**
         * 释放资源
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            super.close();
        }

        public MyWordParallelSource(List<String> word) {
            this.words = word;
        }


        @Override
        public void run(SourceContext ctx) throws Exception {
            int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
            while (!isStop) {
                if (words != null && words.size() > 0) {
                    words.forEach(word -> ctx.collect(numberOfParallelSubtasks + "--->" + word));
                }
                Thread.sleep(2000);
            }
        }

        /**
         * 该方法对无界数据流有作用
         */
        @Override
        public void cancel() {
            isStop = true;
        }
    }

    static class MyWordSource extends RichSourceFunction<String> {
        private List<String> words;

        private boolean flag = true;

        public MyWordSource(List<String> words) {
            this.words = words;
        }

        /**
         * 产生数据，用sourceContext将数据发生出去
         *
         * @param sourceContext
         * @throws Exception
         */
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            //获取当前subTask的运行上下文
            RuntimeContext runtimeContext = getRuntimeContext();
            int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();

            while (flag) {
                if (words != null && words.size() > 0) {
                    words.forEach(word -> sourceContext.collect(indexOfThisSubtask + "--->" + word));
                }

                Thread.sleep(2000);
            }


        }

        /**
         * 停止source,对于无界数据流有用
         */
        @Override
        public void cancel() {
            flag = false;
        }
    }
}
