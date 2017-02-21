package com.spark.java.examples;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.spark.java.utils.Utils;

public class GroupByExample {

	public static void main(String[] args) {
		final JavaSparkContext context = Utils.getSparkContext();

		try {
			// Parallelized with 2 partitions
			final JavaRDD<String> rddX = context.parallelize(Arrays.asList("Joseph", "Jimmy", "Tina", "Thomas", "James",
					"Cory", "Christine", "Jackeline", "Juan"), 3);

			final JavaPairRDD<Character, Iterable<String>> groupByRDD = rddX.groupBy(word -> word.charAt(0));

			System.out.println("groupByRDD  -->>" + groupByRDD.collect());

		} catch (final Exception ex) {
			ex.printStackTrace();
		} finally {
			context.close();
		}
	}
}
