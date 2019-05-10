package org.fbertos.spark;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class SparkDriverProgram {
    @SuppressWarnings("unchecked")
	public static void main(String args[]) throws FileNotFoundException, UnsupportedEncodingException {
    	String file_path = "hdfs://localhost:9000/examples/apache-log04-aug-31-aug-2011-nasa.log";
    	String ip_file_path = "hdfs://localhost:9000/examples/ip-nasa.log";
    	String out_path = "hdfs://localhost:9000/examples/output.log";
        SparkConf conf = new SparkConf().setAppName("spark-example");
        
        //PrintWriter writer = new PrintWriter(out_path, "UTF-8");
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> lines = sc.textFile(file_path);
        JavaRDD<String> ips = sc.textFile(ip_file_path);
        
        JavaPairRDD<String, String> log_pairs = lines.mapToPair(w -> {
        	String[] row = w.split(" ");
        	return new Tuple2<String, String>(row[0], "1");
        });
        
        JavaPairRDD<String, String> ip_pairs = ips.mapToPair(w -> {
        	String[] row = w.split(";");
        	return new Tuple2<String, String>(row[0], row[1]);
        });
        
        JavaPairRDD<String, Tuple2<String, String>> rddWithJoin = log_pairs.join(ip_pairs);

        JavaRDD<String> res = rddWithJoin.map(w -> {
        	return w._1 + ';' + w._2._1 + ';' + w._2._2;
        	
        });
        
        res.saveAsTextFile(out_path);
        
        // writer.println("Total join lines in log file " + rddWithJoin.count());
        
        //writer.println("Total lines in log file " + lines.count());
        
        /* Count all rows with ip inside manually */
        /*
        JavaRDD<Integer> lineCharacters2 = lines.map(s -> { 
        	if (s.indexOf("131.94.128.88")>=0) return 1; 
        	return 0;	
        });*/
        
        /* Filter rows with ip inside */
        /*
        JavaRDD<String> lineCharacters3 = lines.filter(s -> { 
        	if (s.indexOf("131.94.128.88")>=0) return true; 
        	return false;	
        });
        */

        /* Save into a text file */
        //lineCharacters3.saveAsTextFile(out_path + ".1");
        
        /* Map operation -> Mapping number of characters into each line as RDD */
        //JavaRDD<Integer> lineCharacters = lines.map(s -> s.length());
        
        /* Reduce operation -> Calculating total characters */
        //int totalCharacters = lineCharacters.reduce((a, b) -> a + b);
        
        //writer.println("Total characters in log file " + totalCharacters);
        
        /* Reduce operation -> checking each line for .html character pattern */
        /*
        writer.println("Total URL with html extension in log file " 
                + lines.filter(oneLine -> oneLine.contains(".html")).count());
        */
        
        /* Reduce operation -> checking each line for .gif character pattern */
        /*
        writer.println("Total URL with gif extension in log file "
                + lines.filter(oneLine -> oneLine.contains(".gif")).count());
        */
        
        sc.close();
        //writer.close();        
    }
}