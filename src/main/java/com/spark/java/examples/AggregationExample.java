package com.spark.java.examples;

import org.apache.spark.api.java.JavaSparkContext;

import com.spark.java.utils.Utils;

/**
 * The Class AggregationExample.
 */
public class AggregationExample {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {

		final JavaSparkContext context = Utils.getSparkContext();

		try {

			
		} catch (final Exception ex) {
			ex.printStackTrace();
		} finally {
			context.close();
		}
	}
}
