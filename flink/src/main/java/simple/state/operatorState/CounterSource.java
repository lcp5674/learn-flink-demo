package simple.state.operatorState;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @Author:lcp
 * @CreateTime:2021-01-22 21:20:59
 * @Mark:Source端使用State需要注意：为了保证精准一次语义，需要在获取Context的时候加锁
 **/
public class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

    private Long offset = 0L;

    private volatile boolean isRunning = true;

    private ListState<Long> state;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.clear();
        state.add(offset);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("state", LongSerializer.INSTANCE));

        //当发生restore时，则将上次保存的值重新赋值
        for (long l : state.get()) {
            offset = l;
        }
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        final Object lock = ctx.getCheckpointLock();
        while (isRunning) {
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
                Thread.sleep(3000);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
