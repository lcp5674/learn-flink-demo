package simple.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @Author:lcp
 * @CreateTime:2020-10-13 08:41:06
 * @Mark:
 **/
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 8888);

        //方式一：调用原生自带的map算子
//        SingleOutputStreamOperator<String> wordSource = socketTextStream.map(word -> word.toUpperCase());

        //方式二：调用底层的transform算子重定义实现
//        SingleOutputStreamOperator<String> wordSource = socketTextStream.transform("MyMap", TypeInformation.of(String.class), new StreamMap<>(String::toUpperCase));

        //方式三：继承实现类自定义实现
        SingleOutputStreamOperator<String> wordSource = socketTextStream.transform("MyStreamMap", TypeInformation.of(String.class), new MyStreamMap());

        //方式四：实现RichMapFunction类
        SingleOutputStreamOperator<String> map = socketTextStream.map(new RichMapFunction<String, String>() {
            /**
             * 在构造对象之后，执行map方法之前执行一次
             * 通常用于初始化工作，例如连接创建等
             * @param parameters
             * @throws Exception
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            /**
             * 在关闭subtask之前执行一次，例如做一些释放资源的工作
             * @throws Exception
             */
            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public String map(String s) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

                return indexOfThisSubtask + ":" + s.toUpperCase();
            }
        });

        wordSource.print();
        env.execute();
    }

    /**
     * 类似于StreamMap操作
     */
    static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String elementValue = element.getValue();
            output.collect(element.replace(elementValue.toUpperCase()));
        }
    }
}
