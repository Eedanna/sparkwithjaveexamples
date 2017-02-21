package com.spark.java.examples;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.ipc.specific.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

import com.spark.java.utils.Utils;

/**
 * The Class JSONReader.
 */
public class JSONReader implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * The main method.
	 *
	 * @param args
	 *            the arguments
	 */
	public static void main(String[] args) {

		final JavaSparkContext context = Utils.getSparkContext();

		JSONReader jsonReader = new JSONReader();

		try {
			final JavaRDD<String> textFileLines = context.textFile(Utils.getJSONFile());
			System.out.println("textFileLines  -->> " + textFileLines.collect());

			JavaRDD<Person> readerResult = textFileLines.mapPartitions(jsonReader.new ParseJson());
			// JavaRDD<String> writerResult =
			// result.mapPartitions(jsonReader.new WriteJson());

			System.out.println("Result   -->> " + readerResult.collect());
			// formatted.saveAsTextFile(outfile);

		} catch (final Exception ex) {
			ex.printStackTrace();
		} finally {
			context.close();
		}
	}

	class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
		private static final long serialVersionUID = 1L;

		public Iterable<Person> call(Iterator<String> textFileLines) throws Exception {
			ArrayList<Person> people = new ArrayList<Person>();
			ObjectMapper mapper = new ObjectMapper();
			while (textFileLines.hasNext()) {
				String line = textFileLines.next();
				try {
					people.add(mapper.readValue(line, Person.class));
				} catch (Exception e) {
					// skip records on failure
				}
				System.out.println("people -->> " + people);
			}

			return people;
		}
	}

	class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
		private static final long serialVersionUID = 1L;

		public Iterable<String> call(Iterator<Person> people) throws Exception {
			ArrayList<String> text = new ArrayList<String>();
			ObjectMapper mapper = new ObjectMapper();
			while (people.hasNext()) {
				Person person = people.next();
				text.add(mapper.writeValueAsString(person));
			}
			return text;
		}
	}
}
