package com.example;

import java.util.HashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;



public class Main {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
        .setAppName("Java Cassandra Test")
        .setMaster("local")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.cassandra.connection.port", "2020");

        SparkSession spark = new SparkSession.Builder().config(conf).getOrCreate(); 

        Dataset<Row> df = spark.read()
                .format("org.apache.spark.sql.cassandra")
                .options(
                        new HashMap<String, String>() {
                            {
                                put("keyspace", "data");
                                put("table", "imdb");
                            }
                        })
                .load();

        // df.createOrReplaceGlobalTempView("movies");
        // Dataset<Row> highlyRatedMovies = spark.sql("SELECT * FROM imdb ORDER BY averagerating DESC LIMIT 10");
        // highlyRatedMovies.show();


        

        Dataset<Row> sortedRows = df.orderBy(functions.desc("averagerating"));
        sortedRows.show(5, true);

        Dataset<Row> debug = df.orderBy(functions.desc("runtimeminues"));
        debug.show(5, true);

        
        df.show(50, false);
        


    }
}