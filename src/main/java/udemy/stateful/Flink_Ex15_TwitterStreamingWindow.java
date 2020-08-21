package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.util.Properties;

public class Flink_Ex15_TwitterStreamingWindow {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "");
        props.setProperty(TwitterSource.TOKEN, "");
        props.setProperty(TwitterSource.TOKEN_SECRET, "");

        DataStream<Tuple2<String, Integer>> tweets = env.addSource(new TwitterSource(props))
                .map(new LocationCount())
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(1);

        tweets.print();

        env.execute("Twitter streaming window");
    }

    public static class LocationCount implements MapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser; // transient <=> not part of serialization

        public Tuple2<String, Integer> map(String value) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            String location = jsonNode.has("user") && jsonNode.get("user").has("location")
                    ? jsonNode.get("user").get("location").traverse().getValueAsString() : "unknown";
            return new Tuple2<>(location, 1);
        }
    }

}
