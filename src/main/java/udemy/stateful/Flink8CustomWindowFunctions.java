package udemy.stateful;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import udemy.utils.Pojos;
import udemy.utils.StreamUtil;

import java.util.HashMap;
import java.util.Map;

// input: course(String), country (String), timestamp (any input)
public class Flink8CustomWindowFunctions {

    public static void main( String[] args ) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> dataStream = StreamUtil.getDataStream(env, params);

        // 1) Create a custom window of type GlobalWindow, with key type Tuple, and element type CourseCount2
        WindowedStream<Pojos.CourseCount2, Tuple, GlobalWindow> windowedStream = dataStream
                .map(new Pojos.ParseRow2())
                .keyBy("staticKey")
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
        Tuple: Key data type
        Global window: Window type
     */
    public static class CollectUS implements WindowFunction<Pojos.CourseCount2, Map<String, Integer>,
            Tuple, GlobalWindow>{
        public void apply(Tuple tuple,
                          GlobalWindow globalWindow,
                          Iterable<Pojos.CourseCount2> iterable,
                          Collector<Map<String, Integer>> collector
                          ) throws Exception {
            Map<String, Integer> output = new HashMap<>();
            for(Pojos.CourseCount2 signUp : iterable) {
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
