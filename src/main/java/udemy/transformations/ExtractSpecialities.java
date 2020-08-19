package udemy.transformations;

import org.apache.flink.api.common.functions.MapFunction;

public class ExtractSpecialities implements MapFunction<String, String> {

    public String map(String input) {
        try {
            return input.split(",")[1].trim();
        } catch (Exception e) {
            return null;
        }
    }

}