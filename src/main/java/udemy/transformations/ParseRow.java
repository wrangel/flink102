package udemy.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import udemy.utils.Pojos;

public class ParseRow {

    public static class ParseRow1 implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new Tuple2<>(
                        rowData[0].trim(),
                        Integer.parseInt(rowData[1].trim())
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
    }

    // Produces a POJO from a comma delimited element (flat file: row)
    public static class ParseRow2 implements MapFunction<String, Pojos.CourseCount1> {
        public Pojos.CourseCount1 map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new Pojos.CourseCount1(
                        rowData[0].trim(),
                        Integer.parseInt(rowData[1].trim())
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
    }

    public static class ParseRow3 implements MapFunction<String, Pojos.CourseCount2> {
        public Pojos.CourseCount2 map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new Pojos.CourseCount2(
                        rowData[0].trim(),
                        rowData[1].trim(),
                        Integer.parseInt(rowData[2].trim())
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
    }

    public static class ParseRow4 implements MapFunction<String, Tuple2<String, Double>> {
        public Tuple2<String, Double> map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new Tuple2<>(
                        rowData[0].trim(),
                        Double.parseDouble(rowData[1].trim())
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
    }

}
