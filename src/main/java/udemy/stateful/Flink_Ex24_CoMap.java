package udemy.stateful;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import udemy.transformations.ParseRow;
import udemy.utils.StreamUtil;

/* coMap allows to take two streams and treat them as a single one
    - "connect" is similar to "union", but allows to track the origin of each stream entity
    - this allows for creating functionality AFTER connecting, in stead of BEFORE union
 */
public class Flink_Ex24_CoMap {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> stream1 = StreamUtil.getDataStream(env, params);
        DataStream<String> stream2 = env.socketTextStream("localhost", 9000);

        DataStream<Tuple2<String, Double>> coMapStream = stream1.connect(stream2)
                .map(new ParseRow.ParseRow5());

        coMapStream.print();

        env.execute("CoMap");
    }

}
