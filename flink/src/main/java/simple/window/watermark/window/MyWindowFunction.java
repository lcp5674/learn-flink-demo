package simple.window.watermark.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author:lcp
 * @CreateTime:2021-01-13 10:14:54
 * @Mark:
 **/
public class MyWindowFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
    private static final SimpleDateFormat simpleFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:sss");


    @Override
    public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(window.getStart());
        String startTime = simpleFormatter.format(calendar.getTime());
        calendar.setTimeInMillis(window.getEnd());
        String endTime = simpleFormatter.format(calendar.getTime());
        System.out.println("\t\t\t第四步：开始调用MyWindowFunction.apply方法，接收Key--->" + s + " 该key落入窗口的起始时间--->[" + startTime + "," + endTime + ")");
        input.forEach(t -> out.collect(t));
    }
}
