package udemy.stateful;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import udemy.transformations.StreamKeySelectors;
import udemy.utils.StreamUtil;

import static udemy.transformations.ParseRow.ParseRow4;

/* Collect all values with the same key within a certain window, from two distinct streams
    Then, combine all the values from the streams
    Inputs: Streams on different ports of localhost, one with name of employee and # hours, the other with
        name of employee and # products created
 */
public class Flink_Ex23_CoGroup {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Tuple2<String, Double>> stream1 = StreamUtil.getDataStream(env, params)
                .map(new ParseRow4());
        DataStream<Tuple2<String, Double>> stream2 = env.socketTextStream("localhost", 9000)
                .map(new ParseRow4());

        DataStream<Tuple2<String, Double>> coGroupStream = stream1.coGroup(stream2)
                .where(new StreamKeySelectors.StreamKeySelector2()).equalTo(new StreamKeySelectors.StreamKeySelector2())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new AverageProductivity());

        coGroupStream.print();

        env.execute("CoGroup");
    }

    // Template parameter: Data type of first and second input streams, data type of output
    public static class AverageProductivity implements CoGroupFunction<Tuple2<String, Double>,
            Tuple2<String, Double>,
            Tuple2<String, Double>> {
        public void coGroup( // All values from particular window from stream 1
                             Iterable<Tuple2<String, Double>> iterable1,
                             // All values from particular window from stream 2
                             Iterable<Tuple2<String, Double>> iterable2,
                             Collector<Tuple2<String, Double>> collector) throws Exception {
            // Employee name
            String key = null;
            double hours = 0.0;
            double produced = 0.0;
            for (Tuple2<String, Double> hoursRow : iterable1) {
                if (key == null)
                    // Assign the employee name as key
                    key = hoursRow.f0;
                hours = hours + hoursRow.f1;
            }
            for (Tuple2<String, Double> producedRow : iterable2) {
                produced = produced + producedRow.f1;
            }
            collector.collect(Tuple2.of(key, produced / hours));
        }

    }


}
