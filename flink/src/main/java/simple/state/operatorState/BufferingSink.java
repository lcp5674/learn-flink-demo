package simple.state.operatorState;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author:lcp
 * @CreateTime:2021-01-22 21:06:52
 * @Mark:
 **/
public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    private final int threshold;


    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;


    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferedElements.add(value);
        System.out.println("当前缓冲区大小已占用--->"+bufferedElements.size());
        if (bufferedElements.size() == threshold) {
            for (Tuple2<String, Integer> element : bufferedElements) {
                System.out.println("Sink端缓冲区间已满(达到阈值" + threshold + "),输出内容--->" + element);
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    /**
     * @param context:用来初始化Non-keyed state Container，当发生checkpoint时，会把state存储到ListState中
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        checkpointedState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Tuple2<String, Integer>>("buffered-elements", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        })));

        //当失败重启时会先执行该部分逻辑
        if (context.isRestored()) {
            checkpointedState.get().forEach(t -> bufferedElements.add(t));
        }
    }
}
