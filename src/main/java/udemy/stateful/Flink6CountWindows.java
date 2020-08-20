package udemy.stateful;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udemy.utils.Pojos;
import udemy.utils.StreamUtil;

public class Flink6CountWindows {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Count window, only to be used on keyed stream
        DataStream<Pojos.CourseCount1> windowedStream = StreamUtil.getDataStream(env, params)
                .map(new Pojos.ParseRow1())
                .keyBy(Pojos.CourseCount1::getCourse)// using getter
                .countWindow(3)
                .sum("count"); // using public object

        windowedStream.print();

        env.execute("Count windows");
    }

}
