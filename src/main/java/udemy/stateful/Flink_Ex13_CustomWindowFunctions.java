package udemy.stateful;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import udemy.transformations.ParseRow;
import udemy.transformations.ParseRow.ParseRow3;
import udemy.utils.Pojos.CourseCount2;
import udemy.utils.StreamUtil;

import java.util.HashMap;
import java.util.Map;

// input: course(String), country (String), timestamp (any input)
public class Flink_Ex13_CustomWindowFunctions {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // 1) Create a custom window of type GlobalWindow, with key type Tuple, and element type CourseCount2
        WindowedStream<CourseCount2, Tuple1<Integer>, GlobalWindow> windowedStream = StreamUtil
                .getDataStream(env, params)
                .map(new ParseRow3())
                .keyBy(CourseCount2::getStaticKey)
                // Gives the reference count: Out of the last 5 elements, which courses have been booked how many times
                .countWindow(5);

        // Apply a custom window function onto the window stream: Specify how to combine all elements of the window
        DataStream<Map<String, Integer>> outStream = windowedStream.apply(new CollectUS());

        outStream.print();

        env.execute("Custom window functions");
    }

    /*  2) Create a custom combined aggregation and eviction functionality
        -> Within a window, collects all courses in the US and counts their occurrences
        CourseCount2: Input data type
        Map<String, Integer>: Output data type (course name & # times course occurred)
        Tuple1: Key data type (mandatory key type inherits from Tuple)
        Global window: Window type
     */
    public static class CollectUS implements WindowFunction<CourseCount2, Map<String, Integer>,
            Tuple1<Integer>, GlobalWindow>{
        public void apply(Tuple1<Integer> tuple,
                          GlobalWindow globalWindow,
                          Iterable<CourseCount2> iterable,
                          Collector<Map<String, Integer>> collector
                          ) throws Exception {
            Map<String, Integer> output = new HashMap<>();
            for(CourseCount2 signUp : iterable) {
                // Evictor
                if(signUp.country.equals("US")) {
                    // Add or append to Map
                    if(!output.containsKey(signUp.course)) {
                        output.put(signUp.course, 1);
                    }
                    else {
                        output.put(signUp.course, output.get(signUp.course) + 1);
                    }
                }
            }
            collector.collect(output);
        }
    }

}
