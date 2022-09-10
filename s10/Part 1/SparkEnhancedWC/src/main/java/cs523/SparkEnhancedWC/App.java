package cs523.SparkEnhancedWC;

import java.util.Arrays;
import java.util.Scanner;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) throws Exception {
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName(
				"wordCount").setMaster("local"));

		// User Threshold...
		// Part A
		System.out.println("Please enter threshold value");
		int _threshold = 0;
		try {
			// Get Threshold
			Scanner scanner = new Scanner(System.in);
			_threshold = scanner.nextInt();
			scanner.close();
			if (_threshold <= 0)
				throw new Exception("Invalid threshold value");

		} catch (Exception ex) {

		}
		// has to be final to avoid enclosing warning....
		final int threshold = _threshold;

		// File Input
		// Part B
		JavaRDD<String> lines = sc.textFile(args[0]);

		JavaPairRDD<String, Integer> popularWords = lines
				.flatMap(line -> Arrays.asList(line.split(" ")))
				.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y).filter(w -> (w._2 > threshold));

		// Iterating HashMap through for loop
		// Part C
		System.out.println("Highest...");
		for (Tuple2<String, Integer> test : popularWords.collect()) {
			System.out.println(test._1 + "\t" + test._2);
		}

		// Part D
		
		
		
		// Save the word count back out to a text file, causing evaluation
		popularWords.saveAsTextFile(args[1]);
		//Close Spark Context
		sc.close();
	}
}
