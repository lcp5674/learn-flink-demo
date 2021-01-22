package simple.state.keyedState;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @Author:lcp
 * @CreateTime:2021-01-20 13:24:16
 * @Mark:实现去重处理 样例数据：
 * user1&spark
 * user2&flink
 * user1&hive
 * user2&java
 * user1&spark
 * user2&hive
 **/
public class ListStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        env.setParallelism(1);

        socketTextStream.keyBy(t -> t.split("&")[0])
                .flatMap(new RichFlatMapFunction<String, String>() {
                    private transient ListState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getListState(new ListStateDescriptor<String>("distinct", String.class));
                        super.open(parameters);
                    }

                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String str = value.split("&")[1];
                        StringBuilder stringBuilder = new StringBuilder();
                        Iterator<String> iterator = state.get().iterator();
                        while (iterator.hasNext()) {
                            String stateValue = iterator.next();
                            stringBuilder.append(stateValue).append("\t");
                        }

                        String stateStr = stringBuilder.toString().trim();
                        if ((stateStr.length() > 0 && !stateStr.contains(str)) || stateStr.length() == 0) {
                            state.add(str);
                            stateStr += "\t" + str;
                        }
                        out.collect("当前Key为：" + value.split("&")[0] + " 中间状态值为--->" + stateStr);
                    }

                }).print();

        env.execute();
    }
}
