package udemy.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Average implements MapFunction<Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
    public Tuple2<String, Double> map(Tuple3<String, Double, Integer> input) {
        return new Tuple2<>(
                input.f0,
                input.f1 / input.f2
        );
    }
}