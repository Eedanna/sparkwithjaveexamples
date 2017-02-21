package com.spark.java.utils;

import java.io.File;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * The Class Utils.
 */
public class Utils {

	/**
	 * Gets the file.
	 *
	 * @return the file
	 */
	public static String getFile() {
		String inputFilePath = "";
		try {
			final ClassLoader classLoader = Utils.class.getClassLoader();
			final File file = new File(classLoader.getResource("loremipsum.txt").getFile());
			inputFilePath = file.getPath();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return inputFilePath;
	}

	/**
	 * Gets the JSON file.
	 *
	 * @return the JSON file
	 */
	public static String getJSONFile() {
		String inputFilePath = "";
		try {
			final ClassLoader classLoader = Utils.class.getClassLoader();
			final File file = new File(classLoader.getResource("sample.json").getFile());
			inputFilePath = file.getPath();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return inputFilePath;
	}

	/**
	 * Gets the spark context.
	 *
	 * @return the spark context
	 */
	public static JavaSparkContext getSparkContext() {
		final SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local");
		final JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;
	}
}
