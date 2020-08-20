package udemy.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;

public class Pojos {

    /* Create special POJO object for Flink
    Specifications:
    - Class must be public
    - Class must have a public constructor without arguments (A) <=> only working with deprecated "keyBy(String)"
    - Fields either have to be public or they need to have getters and setters (C) <=> works with current keyBy implementation
    - If the non-public field is called foo getter and setter must be called getFoo() and setFoo()
     */
    public static class CourseCount1 {
        public String course;
        public Integer count; // (B)

        // (C)
        public String getCourse() {
            return course;
        }

        // (A)
        public CourseCount1(){
        }

        public CourseCount1(String course, Integer count) {
            this.course = course;
            this.count = count;
        }

        public String toString() {
            return course + ": " + count;
            }
    }

    public static class CourseCount2 {
        // used to group all events into one window (countWindows can only be applied to keyed streams / no windowAll   )
        public Integer staticKey = 1;
        public String course;
        public String country;
        public Integer count;

        public CourseCount2(){
        }

        public CourseCount2(String course, String country, Integer count) {
            this.course = course;
            this.country = country;
            this.count = count;
        }

        public String toString() {
            return course + ": " + count;
        }

    }

    // Produces a POJO from a comma delimited element (flat file: row)
    public static class ParseRow1 implements MapFunction<String, CourseCount1> {
        public CourseCount1 map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new CourseCount1(
                        rowData[0].trim(),
                        Integer.parseInt(rowData[1].trim())
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
    }

    public static class ParseRow2 implements MapFunction<String, CourseCount2> {
        public CourseCount2 map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new CourseCount2(
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

}
