package udemy.stateful;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import udemy.transformations.ParseRow;
import udemy.transformations.ParseRow.ParseRow2;
import udemy.utils.Pojos.CourseCount1;
import udemy.utils.StreamUtil;

public class Flink_Ex12_SessionWindows {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<CourseCount1> outStream = StreamUtil.getDataStream(env, params)
            .map(new ParseRow2())
                .keyBy(CourseCount1::getCourse)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                .sum("count");

        outStream.print();

        env.execute("Session windows");
    }

}
