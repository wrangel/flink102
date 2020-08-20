package udemy.stateful;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import udemy.utils.Pojos;
import udemy.utils.StreamUtil;

public class Flink7SessionWindows {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Pojos.CourseCount1> outStream = StreamUtil.getDataStream(env, params)
            .map(new Pojos.ParseRow1())
                .keyBy(Pojos.CourseCount1::getCourse)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .sum("count");

        outStream.print();

        env.execute("Session windows");
    }

}
