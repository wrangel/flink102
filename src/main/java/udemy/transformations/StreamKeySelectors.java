package udemy.transformations;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class StreamKeySelectors {

    // Select the key out of a tuple
    public static class StreamKeySelector1 implements KeySelector<Tuple3<String, String, Double>, String> {
        public String getKey(Tuple3<String, String, Double> value) {
            return value.f0;
        }
    }

    public static class StreamKeySelector2 implements KeySelector<Tuple2<String, Double>, String> {
        public String getKey(Tuple2<String, Double> value) {
            return value.f0;
        }
    }

}
