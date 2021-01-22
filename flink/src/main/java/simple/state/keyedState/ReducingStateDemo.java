package simple.state.keyedState;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author:lcp
 * @CreateTime:2021-01-20 13:24:25
 * @Mark:统计每日总量
 * 2020-10-12&user1||10
 * 2020-10-20&user2||10
 * 2020-10-21&user3||40
 * 2020-10-13&user1||20
 * 2020-10-14&user5||40
 * 2020-10-17&user3||50
 * 2020-10-12&user2||10
 * 2020-10-12&user2||40
 **/
public class ReducingStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.keyBy(t -> t.split("&")[0])
                .process(new KeyedProcessFunction<String, String, Tuple2<String, Integer>>() {
                    private transient ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>("reduce", new ReduceFunction<Integer>() {
                            @Override
                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Integer.class));
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Integer cnt = Integer.valueOf(value.split("&")[1].split("\\|\\|")[1]);
                        reducingState.add(cnt);
                        out.collect(Tuple2.of(value.split("&")[0], reducingState.get()));
                    }
                }).print();

        env.execute();

    }
}
