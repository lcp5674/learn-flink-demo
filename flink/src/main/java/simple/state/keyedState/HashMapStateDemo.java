package simple.state.keyedState;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;

/**
 * @Author:lcp
 * @CreateTime:2020-12-07 22:55:39
 * @Mark:1.是否能够正确计算？ 2.是否支持容错？不支持，因为hashmap是存储在内存中的，一旦作业重启，则该状态值就是丢失
 **/
public class HashMapStateDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);

        socketTextStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            private HashMap<String, Integer> stateMap = new HashMap<>();

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                if ("error".equalsIgnoreCase(value)) {
                    return Tuple2.of(value, 1 / 0);
                }

                if (stateMap.get(value) != null) {
                    stateMap.put(value, stateMap.get(value) + 1);
                } else {
                    stateMap.put(value, 0);
                }
                return Tuple2.of(value, stateMap.get(value));
            }
        }).print();

        env.execute();

    }
}
