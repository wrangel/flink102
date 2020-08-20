package udemy.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

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
        public Tuple1<Integer> staticKey = new Tuple1<>(1);
        ////public Integer staticKey = 1;
        public String course;
        public String country;
        public Integer count;

        public Tuple1<Integer> getStaticKey() {
            return staticKey;
        }

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

}
