package udemy.stateful;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.util.Collector;
import udemy.transformations.ParseRow;
import udemy.transformations.ParseRow.ParseRow1;
import udemy.utils.Pojos;
import udemy.utils.StreamUtil;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Flink_Ex14_TimeNotions {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        // Set time notion to event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Assign timestamp extractor
        DataStream<String> dataStream = StreamUtil.getDataStream(env, params)
                .assignTimestampsAndWatermarks(new TimestampExtractor());

        DataStream<Tuple2<String, Integer>> outStream = dataStream
                .map(new ParseRow1())
                .keyBy(tuple -> tuple.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(1);

        outStream.print();

        env.execute("Time notions");
    }

    /* Implement interface that allows for extracting timestamps from incoming events
        1) Extract timestamp
        2) Extract watermark (<=> special timestamps which represent progress of time of datastream.
            They thus allow Flink to decide on lateness of events / out-of-orderness of events
     */
    public static class TimestampExtractor implements Serializable, AssignerWithPunctuatedWatermarks<String> {
        public long extractTimestamp(String s, long l) {
            long k1 = Long.parseLong(s.split(",")[2].trim());
            System.out.println("Timestamp: " + k1);
            return k1;
        }
        @Nullable
        public Watermark checkAndGetNextWatermark(String s, long l) {
            long k2 = Long.parseLong(s.split(",")[2].trim());
            System.out.println("Watermark: " + k2);
            return new Watermark(k2);
        }
    }

}
