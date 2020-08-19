package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import udemy.utils.Pojos;
import udemy.utils.StreamUtil;

// --input /Users/matthiaswettstein/sandbox/material/specialities.txt, oder socket (nc -l 8081)
public class Flink7SessionWindows {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        DataStream<Pojos.CourseCount> outStream1 = dataStream
            .map(new Pojos.ParseRow())
                .keyBy(Pojos.CourseCount::getCourse)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .sum("count");

        outStream1.print();

        env.execute("map and flatmap");
    }

}
