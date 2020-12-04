package simple.window.trigger;

import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author:lcp
 * @CreateTime:2020-12-03 17:58:34
 * @Mark:按照计数和event_time来触发计算 当在一个窗口内数据元素个数达到一定阈值后，则触发一次计算，并清除状态;这里不涉及WaterMark逻辑
 * 即理想状态下,事件都是有序的
 **/
public class CustomCountEventTimeTrigger extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 1L;
    private static final int MAX_VALUE = 2;
    private static int count = 1;


    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        long nextEndTime = window.getEnd() - 1;

        System.out.println("当前事件时间:" + timestamp + " 窗口截止时间:" + nextEndTime + " 当前窗口内元素个数为：" + count);

        if (nextEndTime <= ctx.getCurrentWatermark()) {
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            ctx.registerEventTimeTimer(nextEndTime);
        }

        //判断元素个数是否达到阈值
        if (count < MAX_VALUE) {
            count++;
        } else {
            count = 1;
            System.out.println("元素个数达到阈值,开始触发计算：" + element);
            return TriggerResult.FIRE_AND_PURGE;
        }
        return TriggerResult.CONTINUE;
    }


    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("触发processingTime");
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        long nextEndTime = window.getEnd() - 1;

        //当event_time>=指定的触发时间时才会触发计算
        if (time >= nextEndTime) {
            ctx.registerEventTimeTimer(nextEndTime);
            System.out.println("达到窗口结束时间:" + nextEndTime + ",当前事件时间:" + time);
            return TriggerResult.FIRE_AND_PURGE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {

        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.getEnd() - 1);
    }

    public static CustomCountEventTimeTrigger of() {
        return new CustomCountEventTimeTrigger();
    }

}
