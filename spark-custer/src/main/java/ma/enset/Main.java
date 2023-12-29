package ma.enset;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkSession ss = SparkSession.builder()
                .appName("SPRAK with Custer docker")
                .master("spark://spark-master:7077")
//                .master("local[*]")
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(ss.sparkContext());
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> rdd = sparkContext.parallelize(data);
        long count = rdd.count();
        System.out.println("============================ "+ count);
        sparkContext.stop();

    }
}