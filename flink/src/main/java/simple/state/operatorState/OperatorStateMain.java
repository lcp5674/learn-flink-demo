package simple.state.operatorState;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Author:lcp
 * @CreateTime:2021-01-22 21:27:11
 * @Mark:
 **/
public class OperatorStateMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000L);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        env.addSource(new CounterSource())
                .map(t -> Tuple2.of(simpleDateFormat.format(calendar.getTime()), Integer.valueOf(Math.toIntExact(t))))
                .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class))
                .addSink(new BufferingSink(10));

        env.execute();

    }
}
