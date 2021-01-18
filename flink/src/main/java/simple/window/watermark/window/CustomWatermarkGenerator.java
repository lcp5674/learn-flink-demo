package simple.window.watermark.window;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author:lcp
 * @CreateTime:2021-01-14 12:21:52
 * @Mark:
 **/
public class CustomWatermarkGenerator implements WatermarkGenerator<String> {
    private final Long MAX_DURATON = Time.minutes(1L).toMilliseconds();
    private Long maxTimestamp;

    public CustomWatermarkGenerator() {
        this.maxTimestamp = Long.MIN_VALUE + MAX_DURATON + 1;
    }

    @Override
    public void onEvent(String event, long eventTimestamp, WatermarkOutput output) {
        this.maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTimestamp - MAX_DURATON - 1));
    }
}
