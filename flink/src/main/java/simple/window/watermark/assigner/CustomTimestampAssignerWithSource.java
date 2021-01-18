package simple.window.watermark.assigner;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Author:lcp
 * @CreateTime:2021-01-14 14:34:46
 * @Mark:
 **/
public class CustomTimestampAssignerWithSource implements SerializableTimestampAssigner<String> {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final long serialVersionUID = 2004887044308824356L;
    private static final String SEPATOR = "&";

    @Override
    public long extractTimestamp(String element, long recordTimestamp) {
        String eventTime = element.split(SEPATOR)[1];
        long time = System.currentTimeMillis();
        try {
            time = dateFormat.parse(eventTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return time;
    }
}
