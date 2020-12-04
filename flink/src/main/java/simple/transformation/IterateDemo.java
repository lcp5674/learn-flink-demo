package simple.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author:lcp
 * @CreateTime:2020-10-20 09:42:39
 * @Mark:
 **/
public class IterateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Long> longSingleOutputStreamOperator = socketTextStream.map(Long::parseLong);

        IterativeStream<Long> iterate = longSingleOutputStreamOperator.iterate();
        SingleOutputStreamOperator<Long> feedBack = iterate.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                System.out.println("Iterate Map Output==>" + aLong);
                return aLong - 1;
            }
        });

        SingleOutputStreamOperator<Long> filter = feedBack.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong > 0;
            }
        });
        iterate.closeWith(filter);

        feedBack.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong <= 0;
            }
        }).print();

        env.execute();


    }
}
