package simple.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:lcp
 * @CreateTime:2020-10-12 10:12:28
 * @Mark:
 **/
public class TextFileSourceDemo {
    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> textSource = env.readTextFile("/data/GitCode/flink/src/main/resources/word.txt");

        int parallelism = textSource.getParallelism();
        System.out.println("textFile并行度:" + parallelism);

        textSource.print();

        env.execute("TextFileSourceDemo");
    }
}
