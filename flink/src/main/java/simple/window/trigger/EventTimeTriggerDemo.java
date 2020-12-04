package simple.window.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author:lcp
 * @CreateTime:2020-12-04 03:58:15
 * @Mark:复用EventTimeTrigger逻辑，验证执行先后顺序
 **/
public class EventTimeTriggerDemo extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;

    private EventTimeTriggerDemo() {
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("调用onElement方法...");
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        System.out.println("调用onEventTime方法...");
        return time == window.maxTimestamp() ?
                TriggerResult.FIRE :
                TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("调用clear方法....");
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        System.out.println("调用onMerge方法....");
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "EventTimeTriggerDemo()";
    }


    public static EventTimeTriggerDemo create() {
        return new EventTimeTriggerDemo();
    }
}

