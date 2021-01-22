package simple.state.keyedState;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Author:lcp
 * @CreateTime:2020-12-08 13:28:16
 * @Mark:ValueState使用,支持容错
 **/
public class ValueStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 6688);
        socketTextStream.map(t -> Tuple2.of(t.split(",")[0], Integer.parseInt(t.split(",")[1])))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    private transient ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        //1.定义状态描述器
                        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("wc-count", Integer.class);

                        //2.获取状态
                        state = getRuntimeContext().getState(valueStateDescriptor);

                        super.open(parameters);
                    }

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

                        if ("error".equalsIgnoreCase(value.f0)) {
                            return Tuple2.of(value.f0, 1 / 0);
                        }
                        if (state.value() != null) {
                            state.update(state.value() + value.f1);
                        } else {
                            state.update(value.f1);
                        }
                        return Tuple2.of(value.f0, state.value());
                    }
                }).print();

        env.execute();

    }
}
