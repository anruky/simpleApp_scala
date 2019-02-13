/*

 */

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;


public class SimpleCount {
    public static void main(String[] args) {
        String logFile = "file:///Users/test.csv"; // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("A"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("B"); }
        }).count();

        System.out.println("Lines with A: " + numAs + ", lines with B: " + numBs);

        sc.stop();
    }
}