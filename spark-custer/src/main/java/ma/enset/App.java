package ma.enset;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF21;

import java.util.Arrays;
import java.util.List;

public class App {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        SparkSession ss = SparkSession.builder()
                .appName("SPRAK with Custer docker")
//                .master("spark://spark-master:7077")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df1 = ss.read().option("inferSchema",true)
                .option("header",true)
                .format("csv")
                .load("/bitnami/prodcuts.csv");

        df1.printSchema();
        df1.show();


    }
}