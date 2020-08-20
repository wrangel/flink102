package udemy.stateful;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udemy.transformations.ParseRow;
import udemy.transformations.ParseRow.ParseRow2;
import udemy.utils.Pojos.CourseCount1;
import udemy.utils.StreamUtil;

public class Flink_Ex11_CountWindows {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Count window, only to be used on keyed stream
        DataStream<CourseCount1> windowedStream = StreamUtil.getDataStream(env, params)
                .map(new ParseRow2())
                .keyBy(CourseCount1::getCourse)// using getter
                .countWindow(3)
                .sum("count"); // using public object

        windowedStream.print();

        env.execute("Count windows");
    }

}
