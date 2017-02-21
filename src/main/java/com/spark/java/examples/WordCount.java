package com.spark.java.examples;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.spark.java.utils.Utils;

import scala.Tuple2;

/**
 * The Class WordCount.
 */
public class WordCount {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 * @throws Exception the exception
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		final Pattern SPACE = Pattern.compile(" ");
		final JavaSparkContext context = Utils.getSparkContext();

		try {
			final JavaRDD<String> textFileLines = context.textFile(Utils.getFile());
			System.out.println("lines  -->> " + textFileLines.count()); // lines  -->> 7

			final JavaRDD<String> words = textFileLines.flatMap(new FlatMapFunction<String, String>() {
				public Iterable<String> call(String s) {
					return Arrays.asList(SPACE.split(s));
				}
			});			
			System.out.println("words -->>"+words.collect()); 
			System.out.println("words -->>"+words.collect().size());  // words -->>432
			
			final JavaPairRDD<String, Integer> wordsInPair = words.mapToPair(new PairFunction<String, String, Integer>() {
				public Tuple2<String, Integer> call(String s) {
					return new Tuple2<String, Integer>(s, 1);
				}
			});			
			System.out.println("wordsInPair -->>"+wordsInPair.collect()); 
			//wordsInPair -->> [(Lorem,1), (ipsum,1), (dolor,1), (sit,1), (amet,,1), (consectetur,1), (adipiscing,1), (elit.,1), (Phasellus,1), (nec,1), (egestas,1), (tellus.,1), (Nunc,1), (efficitur,1), (nunc,1), (nunc.,1), (Fusce,1), (quis,1), (tortor,1), (sapien.,1), (Cras,1), (finibus,1), (nisl,1), (eu,1), (eros,1), (tincidunt,,1), (eget,1), (laoreet,1), (velit,1), (porta.,1), (Morbi,1), (pellentesque,1), (volutpat,1), (mollis.,1), (Quisque,1), (maximus,1), (tellus,1), (ut,1), (magna,1), (vulputate,,1), (at,1), (pharetra,1), (turpis,1), (ultricies.,1), (Donec,1), (eu,1), (quam,1), (justo.,1), (Suspendisse,1), (sit,1), (amet,1), (sollicitudin,1), (orci.,1), (Vivamus,1), (pulvinar,1), (sem,1), (in,1), (risus,1), (pulvinar,1), (dignissim.,1), (Nulla,1), (sit,1), (amet,1), (laoreet,1), (eros.,1), (Nullam,1), (sit,1), (amet,1), (erat,1), (dignissim,,1), (vulputate,1), (sapien,1), (at,,1), (tincidunt,1), (enim.,1), (Etiam,1), (nunc,1), (neque,,1), (condimentum,1), (eu,1), (dui,1), (at,,1), (vestibulum,1), (ornare,1), (odio.,1), (Fusce,1), (sed,1), (dolor,1), (pulvinar,,1), (euismod,1), (mauris,1), (eu,,1), (elementum,1), (purus.,1), (In,1), (gravida,1), (sollicitudin,1), (quam,1), (nec,1), (ultricies.,1), (Aenean,1), (vel,1), (nisl,1), (eget,1), (metus,1), (lobortis,1), (luctus,1), (a,1), (at,1), (erat.,1), (Suspendisse,1), (ut,1), (ipsum,1), (quam.,1), (Mauris,1), (id,1), (justo,1), (non,1), (ligula,1), (aliquam,1), (tristique.,1), (Phasellus,1), (volutpat,1), (quam,1), (at,1), (neque,1), (fringilla,,1), (sed,1), (condimentum,1), (diam,1), (maximus.,1), (Proin,1), (ut,1), (quam,1), (aliquet,,1), (convallis,1), (elit,1), (at,,1), (dignissim,1), (sem.,1), (Nam,1), (eu,1), (arcu,1), (purus.,1), (Cras,1), (et,1), (ligula,1), (ac,1), (mauris,1), (fringilla,1), (semper.,1), (Mauris,1), (interdum,1), (magna,1), (rhoncus,1), (pretium,1), (varius.,1), (Nulla,1), (fermentum,1), (est,1), (erat,,1), (eu,1), (interdum,1), (erat,1), (sodales,1), (nec.,1), (Quisque,1), (ornare,1), (suscipit,1), (eros,,1), (at,1), (tempus,1), (diam,1), (dapibus,1), (tristique.,1), (Morbi,1), (malesuada,1), (nibh,1), (ac,1), (justo,1), (faucibus,1), (volutpat.,1), (Curabitur,1), (nec,1), (lacus,1), (non,1), (neque,1), (euismod,1), (pharetra.,1), (Suspendisse,1), (odio,1), (ipsum,,1), (sodales,1), (vitae,1), (sapien,1), (ut,,1), (porta,1), (feugiat,1), (enim.,1), (Aliquam,1), (erat,1), (volutpat.,1), (Fusce,1), (elementum,1), (posuere,1), (dolor,1), (id,1), (auctor.,1), (Donec,1), (in,1), (ante,1), (pulvinar,,1), (malesuada,1), (purus,1), (non,,1), (tincidunt,1), (dui.,1), (Maecenas,1), (mollis,1), (in,1), (augue,1), (vitae,1), (vulputate.,1), (Donec,1), (condimentum,1), (fringilla,1), (auctor.,1), (Aenean,1), (efficitur,1), (metus,1), (justo,,1), (posuere,1), (placerat,1), (urna,1), (efficitur,1), (eget.,1), (Nullam,1), (et,1), (est,1), (eu,1), (nibh,1), (dapibus,1), (fringilla.,1), (Praesent,1), (lobortis,1), (tincidunt,1), (odio,,1), (nec,1), (dapibus,1), (odio,1), (faucibus,1), (sit,1), (amet.,1), (In,1), (faucibus,,1), (magna,1), (eu,1), (tincidunt,1), (consequat,,1), (velit,1), (risus,1), (bibendum,1), (ligula,,1), (nec,1), (aliquam,1), (nisl,1), (dui,1), (sodales,1), (ligula.,1), (Integer,1), (at,1), (dapibus,1), (metus,,1), (id,1), (pellentesque,1), (mauris.,1), (Vivamus,1), (eleifend,1), (nisi,1), (id,1), (mollis,1), (dapibus.,1), (Donec,1), (ut,1), (ex,1), (sed,1), (mauris,1), (consectetur,1), (feugiat.,1), (Quisque,1), (viverra,1), (quam,1), (purus,,1), (eu,1), (ornare,1), (massa,1), (iaculis,1), (vitae.,1), (Praesent,1), (fringilla,1), (dui,1), (nec,1), (arcu,1), (feugiat,,1), (ac,1), (posuere,1), (dui,1), (ullamcorper.,1), (Suspendisse,1), (nec,1), (velit,1), (a,1), (ipsum,1), (euismod,1), (malesuada,1), (eu,1), (non,1), (nibh.,1), (Mauris,1), (aliquam,1), (quis,1), (quam,1), (sit,1), (amet,1), (condimentum.,1), (Donec,1), (a,1), (sem,1), (dapibus,,1), (pretium,1), (elit,1), (at,,1), (fermentum,1), (dui.,1), (Etiam,1), (arcu,1), (ex,,1), (imperdiet,1), (tempor,1), (ex,1), (a,,1), (convallis,1), (condimentum,1), (erat.,1), (Aliquam,1), (ullamcorper,1), (ultricies,1), (eros,,1), (vitae,1), (cursus,1), (ligula,1), (viverra,1), (in.,1), (Quisque,1), (et,1), (viverra,1), (sem,,1), (eget,1), (vehicula,1), (metus.,1), (Nam,1), (rutrum,1), (leo,1), (quam,,1), (a,1), (vestibulum,1), (diam,1), (auctor,1), (at.,1), (Integer,1), (diam,1), (leo,,1), (consectetur,1), (eget,1), (rhoncus,1), (ac,,1), (facilisis,1), (sit,1), (amet,1), (tellus.,1), (Duis,1), (mattis,1), (placerat,1), (vulputate.,1), (Nunc,1), (eu,1), (aliquet,1), (tellus,,1), (in,1), (varius,1), (erat.,1), (Pellentesque,1), (elementum,1), (cursus,1), (dolor,,1), (condimentum,1), (consectetur,1), (enim,1), (sagittis,1), (ac.,1), (Donec,1), (vehicula,1), (ut,1), (mauris,1), (non,1), (porttitor.,1), (Vivamus,1), (rutrum,1), (nunc,1), (et,1), (egestas,1), (vulputate.,1), (Proin,1), (nec,1), (tempor,1), (velit.,1), (Aliquam,1), (eget,1), (augue,1), (mollis,,1), (cursus,1), (arcu,1), (sed,,1), (tincidunt,1), (nulla.,1), (Aenean,1), (feugiat,1), (arcu,1), (eu,1), (mauris,1), (cursus,1), (gravida.#,1)]

			final JavaPairRDD<String, Integer> wordCount = wordsInPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer i1, Integer i2) {
					return i1 + i2;
				}
			});

			final List<Tuple2<String, Integer>> result = wordCount.collect();
			for (Tuple2<?, ?> tuple : result) {
				System.out.println(tuple._1() + " :::: " + tuple._2());
			}
		} catch (final Exception ex) {
			ex.printStackTrace();
		} finally {
			context.close();
		}
	}
}
