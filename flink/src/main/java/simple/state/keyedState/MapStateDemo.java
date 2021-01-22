package simple.state.keyedState;

import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Author:lcp
 * @CreateTime:2021-01-20 13:24:45
 * @Mark:统计每个用户出现的次数以及每天的UV
 * 样例数据
 * 2020-12-10&user1||dv1
 * 2020-12-10&user1||dv2
 * 2020-12-11&user1||dv3
 * 2020-12-12&user2||dv1
 * 2020-12-13&user3||dv2
 * 2020-12-10&user1||dv5
 * 2020-12-12&user4||dv2
 * 2020-12-11&user2||dv3
 * 2020-12-14&user1||dv5
 * 2020-12-13&user2||dv2
 * 2020-12-10&user5||dv5
 * 2020-12-10&user1||dv3
 * 2020-12-11&user2||dv1
 * 2020-12-10&user3||dv4
 * 2020-12-14&user1||dv4
 **/

public class MapStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        socketTextStream.keyBy(t -> t.split("&")[0])
                .process(new KeyedProcessFunction<String, String, Tuple2<String, Integer>>() {
                    private static final long serialVersionUID = 6805407325918189102L;
                    //MapState用来统计UK出现的次数
                    private transient MapState<String, Integer> mapState;

                    //ValueState用来统计去重后的UK个数
                    private transient ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("mapState", String.class, Integer.class));
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("distinctState", Integer.class));
                        super.open(parameters);
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String str = value.split("&")[1];
                        String uK = str.split("\\|\\|")[0];
                        if (mapState.contains(uK)){
                            mapState.put(uK,mapState.get(uK)+1);
                        }else{
                            mapState.put(uK,1);
                        }
                        List list = IteratorUtils.toList(mapState.keys().iterator());
                        valueState.update(list.size());
                        System.out.println("当前key：" + ctx.getCurrentKey() + "  去重UK统计个数为-->" + valueState.value() + " 当前UK出现的次数--->" + mapState.get(uK));
                        out.collect(Tuple2.of(ctx.getCurrentKey(), valueState.value()));
                    }

                })
                .print();

        env.execute();

    }
}
