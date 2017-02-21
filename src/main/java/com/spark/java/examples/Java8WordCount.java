package com.spark.java.examples;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.java.utils.Utils;

import scala.Tuple2;

/**
 * The Class Java8WordCount.
 */
public class Java8WordCount {

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {
		final JavaSparkContext context = Utils.getSparkContext();

		final JavaRDD<String> textFileLines = context.textFile(Utils.getFile());
		System.out.println("lines  -->> " + textFileLines.count());

		// Filter words and count the number of Occurances.
		JavaRDD<String> wordSearch = textFileLines.filter(s -> s.contains("elementum"));
		System.out.println("wordSearch Count -->> " + wordSearch.count());

		JavaRDD<String> words = textFileLines.flatMap(line -> Arrays.asList(line.split(" ")));
		JavaPairRDD<String, Integer> wordsPair = words.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
				.reduceByKey((x, y) -> x + y);
		System.out.println("Words -->>" + wordsPair.collect());
		wordsPair.saveAsTextFile("resources/output");
	}
}
