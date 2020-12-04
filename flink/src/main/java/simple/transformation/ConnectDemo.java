package simple.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author:lcp
 * @CreateTime:2020-10-15 21:27:23
 * @Mark:
 **/
public class ConnectDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);

        DataStreamSource<String> streamStr = env.fromElements("hello", "str", "good");

        ConnectedStreams<Integer, String> connect = stream1.connect(streamStr);

        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return "hhhh1"+value;
            }

            @Override
            public String map2(String value) throws Exception {
                return "11111"+value;
            }
        }).print();

        env.execute("ConnectDemo");

    }
}
