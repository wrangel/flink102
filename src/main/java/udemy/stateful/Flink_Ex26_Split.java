package udemy.stateful;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udemy.utils.StreamUtil;

import java.util.ArrayList;
import java.util.List;

/* Split splits a single output stream into two output streams
    To apply a split, an Output selector is required
    - input is localhost on 8081, e.g.
 */
public class Flink_Ex26_Split {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> stream = StreamUtil.getDataStream(env, params);
        // Split single input stream in two output streams
        SplitStream<String> split = stream.split(new RerouteData());
        DataStream<String> awords = split.select("Awords");
        DataStream<String> otherwords = split.select("Others");

        awords.print();
        otherwords.print();

        env.execute("Split");
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
