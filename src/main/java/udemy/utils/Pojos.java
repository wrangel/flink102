package udemy.utils;

import org.apache.flink.api.common.functions.MapFunction;

public class Pojos {

    // Special POJO object
    // Class must be public
    // Class must have a public constructor without arguments (A) <=> only working with deprecated "keyBy(String)"
    // Fields either have to be public or they need to have getters and setters (C) <=> works with current keyBy implementation
    // If the non-public field is called foo getter and setter must be called getFoo() and setFoo()
    public static class CourseCount {
        public String course;
        public Integer count; // (B)

        // (C)
        public String getCourse() {
            return course;
        }

        // (A)
        public CourseCount(){
        }

        public CourseCount(String course, Integer count) {
            this.course = course;
            this.count = count;
        }

        public String toString() {return course + ": " + count;}
    }

    public static class ParseRow implements MapFunction<String, CourseCount> {
        public Pojos.CourseCount map(String input) throws Exception {
            try {
                String[] rowData = input.split(",");
                return new Pojos.CourseCount(
                        rowData[0].trim(),
                        Integer.parseInt(rowData[1].trim())
                );
            } catch (Exception e) {
                System.out.println("Exception in ParseRow");
            }
            return null;
        }
    }

}
