package udemy.stateful;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;

/* Vertices and edges
    Flink has a way to represent a DataSet as vertices and edges
    -- vertices <pathToFile> --nodes <pathToFile2>
 */
public class Flink_Ex28_GraphData {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // id, attributes: name and weight of vertex
        DataSet<Tuple2<Long, Tuple2<String, Integer>>> vertexTuples = env.readCsvFile(params.get("vertices"))
                .fieldDelimiter("|")
                .ignoreInvalidLines()
                .types(Long.class, String.class, Integer.class)
                .map(new GetVertex());

        // ids of start and end node, attribute: weight of edge
        DataSet<Tuple3<Long, Long, Integer>> edgeTuples = env.readCsvFile(params.get("edges"))
                .fieldDelimiter(",")
                .ignoreInvalidLines()
                .types(Long.class, Long.class, Integer.class);

        // id of nodes, attribute of the vortex along with the vortex tuple, attribute (weight of the edge)
        Graph<Long, Tuple2<String, Integer>, Integer> graph = Graph.fromTupleDataSet(vertexTuples, edgeTuples, env);

        System.out.println("Number of edges" + graph.numberOfEdges());
        System.out.println("Number of vertices" + graph.numberOfVertices());
    }

    private static class GetVertex implements MapFunction<Tuple3<Long, String, Integer>,
            Tuple2<Long, Tuple2<String, Integer>>
            > {
        public Tuple2<Long, Tuple2<String, Integer>> map(Tuple3<Long, String, Integer> input) throws Exception {
            return Tuple2.of(input.f0, Tuple2.of(input.f1, input.f2));
        }
    }
}
