package udemy.transformations;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class SumAndCounts {

    /* Course, length, area. Objective: Average course length in each area
     Takes two values, cumulated and current, and combines them both, one by one
     */
    public static class SumAndCount1 implements ReduceFunction<Tuple3<String, Double, Integer>> {
        public Tuple3<String, Double, Integer> reduce(
                Tuple3<String, Double, Integer> cumulative,
                Tuple3<String, Double, Integer> input
        ) {
            return new Tuple3<>(
                    input.f0,
                    cumulative.f1 + input.f1,
                    cumulative.f2 + 1
            );
        }
    }

    /* Course, length, area. Objective: Average course length in each area
     Collects all the values with the same key into one group, in one single step
     Two template parameters: input and output data types
     */
    public static class SumAndCount2 implements GroupReduceFunction<
            Tuple3<String, Double, Integer>,
            Tuple3<String, Double, Integer>
            > {
        public void reduce(
                Iterable<Tuple3<String, Double, Integer>> values,
                Collector<Tuple3<String, Double, Integer>> collector
        ) {
            String key = null;
            double sum = 0.0;
            Integer count = 0;
            for (Tuple3<String, Double, Integer> value : values) {
                if ((key == null))
                    key = value.f0;
                sum += value.f1;
                count += value.f2;
            }
            collector.collect(Tuple3.of(key, sum, count));
        }
    }

}
