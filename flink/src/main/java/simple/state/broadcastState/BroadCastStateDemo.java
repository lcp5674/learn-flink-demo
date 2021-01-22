package simple.state.broadcastState;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author:lcp
 * @CreateTime:2020-12-21 18:54:33
 * @Mark:
 **/
public class BroadCastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> broadList = new ArrayList<>();
        broadList.add("spark");
        broadList.add("hadoop");
        broadList.add("flink");

        List<String> demoList = new ArrayList<>();
        demoList.add("a");
        demoList.add("b");
        demoList.add("c");

        final MapStateDescriptor<String, String> BROADCAST = new MapStateDescriptor<String, String>("broadcastKey", String.class, String.class);

        BroadcastStream<String> broadcast = env.addSource(new BroadCastSource(broadList)).broadcast(BROADCAST);

        env.addSource(new MyWordSource(demoList)).connect(broadcast).process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                out.collect(value + "---->" + ctx.getBroadcastState(BROADCAST).get("broadcastKey"));
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                ctx.getBroadcastState(BROADCAST).put("broadcastKey", "广播变量:" + value + " 附加随机数:" + Math.random());
            }
        }).map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return indexOfThisSubtask + "---->" + value;
            }
        }).print();

        env.execute();

    }

    static class BroadCastSource implements SourceFunction<String> {
        private List<String> words;
        private boolean isStop = false;

        public BroadCastSource() {
        }

        public BroadCastSource(List<String> words) {
            this.words = words;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            if (words != null && words.size() > 0) {
                StringBuilder stringBuilder = new StringBuilder();
                while (!isStop) {
                    Iterator<String> iterator = words.iterator();
                    while (iterator.hasNext()) {
                        String next = iterator.next();
                        int index = words.indexOf(next);
                        stringBuilder.append(index + ":" + next);
                        stringBuilder.append(";");
                    }
                    int index = (int) (Math.random() * words.size());
                    System.out.println("索引值：" + index + "--->List值：" + stringBuilder.toString());
                    ctx.collect(words.get(index));
                    Thread.sleep(2000);
                    stringBuilder.delete(0, stringBuilder.length());
                }
            }
        }

        @Override
        public void cancel() {
            isStop = true;
        }
    }

    static class MyWordSource implements SourceFunction<String> {
        private List<String> words;
        private boolean isStop = false;

        public MyWordSource() {
        }

        public MyWordSource(List<String> words) {
            this.words = words;
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            if (words != null && words.size() > 0) {
                while (!isStop) {
                    words.forEach(word -> ctx.collect(word));
                    Thread.sleep(2000);
                }
            }
        }

        @Override
        public void cancel() {
            isStop = true;
        }
    }
}
