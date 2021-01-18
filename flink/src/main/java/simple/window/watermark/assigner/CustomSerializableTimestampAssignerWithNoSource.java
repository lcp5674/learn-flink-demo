package simple.window.watermark.assigner;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * @Author:lcp
 * @CreateTime:2021-01-13 23:33:07
 * @Mark:
 **/
public class CustomSerializableTimestampAssignerWithNoSource implements SerializableTimestampAssigner<Tuple2<Long, String>> {
    private static final long serialVersionUID = 6521693074685759758L;

    @Override
    public long extractTimestamp(Tuple2<Long, String> element, long recordTimestamp) {
        return element.f0;
    }
}
