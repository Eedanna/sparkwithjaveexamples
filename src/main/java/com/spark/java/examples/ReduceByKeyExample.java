package com.spark.java.examples;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import com.spark.java.utils.Utils;

import scala.Tuple2;

/**
 * The Class ReduceByKeyExample.
 */
public class ReduceByKeyExample {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	public static void main(String[] args) throws Exception {
		final JavaSparkContext context = Utils.getSparkContext();

		try {
			// Reduce Function for sum
			final Function2<Integer, Integer, Integer> reduceSumFunc = (x, n) -> (x + n);

			// Parallelized with 2 partitions
			final JavaRDD<String> x = context.parallelize(Arrays.asList("a", "b", "a", "a", "b", "b", "b", "b"), 3);

			// PairRDD parallelized with 3 partitions
			// mapToPair function will map JavaRDD to JavaPairRDD
			final JavaPairRDD<String, Integer> rddX = x.mapToPair(e -> new Tuple2<String, Integer>(e, 1));

			System.out.println("rddX --------->>>>>>>>" + rddX.collect());

			// New JavaPairRDD with reduceByKey
			final JavaPairRDD<String, Integer> rddY = rddX.reduceByKey(reduceSumFunc);

			// Print tuples
			for (Tuple2<String, Integer> element : rddY.collect()) {
				System.out.println("(" + element._1 + " :: " + element._2 + ")");
			}

		} catch (final Exception ex) {
			ex.printStackTrace();
		} finally {
			context.close();
		}
	}
}
