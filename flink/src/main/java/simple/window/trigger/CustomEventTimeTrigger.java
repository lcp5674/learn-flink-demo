package simple.window.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @Author:lcp
 * @CreateTime:2020-12-03 05:08:18
 * @Mark:
 **/
public class CustomEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
    private CustomEventTimeTrigger(long interval) {
        this.interval = interval;
    }

    private final long interval;


    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("fire-time", new Min(), LongSerializer.INSTANCE);


    public static <W extends Window> CustomEventTimeTrigger<W> of(Time interval) {
        return new CustomEventTimeTrigger<W>(interval.toMilliseconds());
    }

    /**
     * 当把一个元素添加到窗口时会调用该方法
     *
     * @param element
     * @param timestamp
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        System.out.println("调用onElement方法...");
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        //对第一条数据事件注册一个定时器
        if (fireTimestamp.get() == null) {

            long start = timestamp - (timestamp % interval);
            long nextFireTimestamp = start + interval;

            //注册一个定时器
            ctx.registerEventTimeTimer(nextFireTimestamp);

            System.out.println("第一条数据coming...." + " start值：" + start + "下次触发时间:" + nextFireTimestamp);

            fireTimestamp.add(nextFireTimestamp);
        }

        return TriggerResult.CONTINUE;
    }

    /**
     * 基于processing time划分窗口并触发时调用该方法
     *
     * @param time
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    /**
     * 基于EventTime划分窗口并触发时调用该方法
     *
     * @param time
     * @param window
     * @param ctx
     * @return
     * @throws Exception
     */
    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        System.out.println("调用onEventTime方法...");
        if (time == window.maxTimestamp()) {
            System.out.println("当等于窗口最大时间即开始触发:" + window.maxTimestamp());
            return TriggerResult.FIRE;
        }

        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

        Long fireTimestamp = fireTimestampState.get();

        //当event_time=指定的触发时间时才会触发计算
        if (fireTimestamp != null && fireTimestamp == time) {
            fireTimestampState.clear();
            fireTimestampState.add(time + interval);
            ctx.registerEventTimeTimer(time + interval);
            System.out.println("下次触发时间:" + (interval + time));
            return TriggerResult.FIRE;
        }

        return TriggerResult.CONTINUE;
    }

    /**
     * 窗口清除
     *
     * @param window
     * @param ctx
     * @throws Exception
     */
    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        System.out.println("调用clear方法");
        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
        Long timestamp = fireTimestamp.get();
        if (timestamp != null) {
            ctx.deleteEventTimeTimer(timestamp);
            fireTimestamp.clear();
        }
    }

    private static class Min implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return Math.min(value1, value2);
        }
    }

}
