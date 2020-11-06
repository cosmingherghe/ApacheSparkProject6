package dev.cosmingherghe.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;

public class Application {

    public static void main(String[] args) {

        //Turn off INFO log entries
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("Learning Spark SQL Dataframe API")
                .master("local")
                .getOrCreate();

        String redditFile = "D:\\_IntelliJ-worksspace\\ApacheSpark\\Files\\RC_2007-01-SmallFile";

        Dataset<Row> redditDf = spark.read().format("json")
                .option("inferSchema", true) // Make sure to use string version of true
                .option("header", true)
                .load(redditFile);

        //find the most commonly used word by doing a count of occurrences
        redditDf = redditDf.select("body");
        Dataset<String> wordsDs = redditDf.flatMap( (FlatMapFunction<Row, String>)
                        r -> Arrays.asList( r.toString().replace("\n", "").trim().toLowerCase()
                        .split(" ")).iterator(),
                Encoders.STRING());

        Dataset<Row> wordsDf = wordsDs.toDF();

    }
}