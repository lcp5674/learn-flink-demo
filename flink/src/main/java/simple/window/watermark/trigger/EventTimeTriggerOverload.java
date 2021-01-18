package simple.window.watermark.trigger;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author:lcp
 * @CreateTime:2021-01-13 10:01:39
 * @Mark:
 **/
@PublicEvolving
public class EventTimeTriggerOverload extends Trigger<Object, TimeWindow> {
    private static final long serialVersionUID = 2764807732507941002L;
    private static final SimpleDateFormat simpledateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Calendar calendar = Calendar.getInstance();
    private long currentElementWatermark;
    private long maxEventTimestamp = Long.MIN_VALUE;

    private EventTimeTriggerOverload() {

    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        TriggerResult result = TriggerResult.FIRE;
        long registerElementWithWatermark = ctx.getCurrentWatermark();
        this.currentElementWatermark = ctx.getCurrentWatermark();
        if (window.maxTimestamp() > registerElementWithWatermark) {
            registerElementWithWatermark = window.maxTimestamp();
            result = TriggerResult.CONTINUE;
        }

        calendar.setTimeInMillis(window.getStart());
        System.out.print("\t\t第三步：开始调用EventTimeTriggerOverload.onElement方法计算水印，接收元素--->" + element
                + " 当前窗口起始时间---->【" + simpledateFormat.format(calendar.getTime()) + ",");
        calendar.setTimeInMillis(window.getEnd());
        System.out.print(simpledateFormat.format(calendar.getTime()) + ")");

        if (timestamp > maxEventTimestamp) {
            this.maxEventTimestamp = timestamp;
            this.currentElementWatermark = this.maxEventTimestamp - Time.minutes(1L).toMilliseconds();
        }

        calendar.setTimeInMillis(this.currentElementWatermark);
        System.out.println(" 当前元素计算得到的水印值--->" + simpledateFormat.format(calendar.getTime()));

        ctx.registerEventTimeTimer(registerElementWithWatermark);
        return result;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
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
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }


    }

    @Override
    public String toString() {
        return "EventTimeTriggerOverload()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static EventTimeTriggerOverload create() {
        return new EventTimeTriggerOverload();
    }
}
