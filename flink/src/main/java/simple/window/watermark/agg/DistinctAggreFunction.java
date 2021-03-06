package simple.window.watermark.agg;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author:lcp
 * @CreateTime:2021-01-12 23:15:19
 * @Mark:
 **/
public class DistinctAggreFunction implements AggregateFunction<Tuple2<String, String>, Tuple2<String, Set<String>>, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Set<String>> createAccumulator() {
        return Tuple2.of("", new HashSet<>());
    }

    @Override
    public Tuple2<String, Set<String>> add(Tuple2<String, String> value, Tuple2<String, Set<String>> accumulator) {
        accumulator.f0 = value.f0;
        accumulator.f1.add(value.f1);
        System.out.println("\t第二步：开始调用DistinctAggreFunction.add方法，并接收数据流--->" + value + "更新中间状态值为--->" + accumulator);
        return accumulator;
    }

    @Override
    public Tuple2<String, Integer> getResult(Tuple2<String, Set<String>> accumulator) {
        return Tuple2.of(accumulator.f0, accumulator.f1.size());
    }

    @Override
    public Tuple2<String, Set<String>> merge(Tuple2<String, Set<String>> a, Tuple2<String, Set<String>> b) {
        a.f1.addAll(b.f1);
        return a;
    }


}
