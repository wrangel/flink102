package udemy.stateful;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import udemy.transformations.Average;
import udemy.transformations.ParseRow;
import udemy.transformations.SumAndCounts;

import java.util.ArrayList;
import java.util.List;

/* DataSet transformations are very analogous to DataStream transformations
    - We go along Example 8 here
    --input <whateverLocation>, file lines of form t,4.9,2
 */
public class Flink_Ex27_DataSetAPI {

    public static void main(String[] args) throws Exception {

        // Use execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Use DataSet API
        DataSet<String> dataSet = env.readTextFile(params.get("input"));

        DataSet<Tuple2<String, Double>> output = dataSet
                .map(new ParseRow.ParseRow6())
                .groupBy(0)
                .reduceGroup(new SumAndCounts.SumAndCount2())
                .map(new Average());

        output.print();

        // Kill this line in the context of DataSet
        //env.execute("DataSet");
    }

    // Triage input elements, and add label the elements
    public static class RerouteData implements OutputSelector<String> {
        public Iterable<String> select(String s) {
            List<String> outputs = new ArrayList<>();
            if (s.startsWith("a"))
                outputs.add("Awords");
            else
                outputs.add("Others");
            return outputs;
        }
    }

}
