package simple.window.watermark.window;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * @Author:lcp
 * @CreateTime:2021-01-13 11:22:44
 * @Mark:
 **/
public abstract class MyBoundedOutOfOrdernessTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> {


    private static final long serialVersionUID = 1L;

    private long currentMaxTimestamp;

    private long lastEmittedWatermark = Long.MIN_VALUE;

    private final long maxOutOfOrderness;

    public MyBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness, Long initEmittedWatermark) {
//        this.lastEmittedWatermark = initEmittedWatermark;

        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the maximum allowed " +
                    "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        }
        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
//        this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
        this.currentMaxTimestamp = initEmittedWatermark;
    }

    public MyBoundedOutOfOrdernessTimestampExtractor(Time maxOutOfOrderness) {
        if (maxOutOfOrderness.toMilliseconds() < 0) {
            throw new RuntimeException("Tried to set the maximum allowed " +
                    "lateness to " + maxOutOfOrderness + ". This parameter cannot be negative.");
        }
        this.maxOutOfOrderness = maxOutOfOrderness.toMilliseconds();
        this.currentMaxTimestamp = Long.MIN_VALUE + this.maxOutOfOrderness;
    }

    public long getMaxOutOfOrdernessInMillis() {
        return maxOutOfOrderness;
    }

    /**
     * Extracts the timestamp from the given element.
     *
     * @param element The element that the timestamp is extracted from.
     * @return The new timestamp.
     */
    public abstract long extractTimestamp(T element);

    @Override
    public final Watermark getCurrentWatermark() {
        // this guarantees that the watermark never goes backwards.
        long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
        if (potentialWM >= lastEmittedWatermark) {
            lastEmittedWatermark = potentialWM;
        }
        return new Watermark(lastEmittedWatermark);
    }

    @Override
    public final long extractTimestamp(T element, long previousElementTimestamp) {
        long timestamp = extractTimestamp(element);
        if (timestamp > currentMaxTimestamp) {
            currentMaxTimestamp = timestamp;
        }
        return timestamp;
    }
}
