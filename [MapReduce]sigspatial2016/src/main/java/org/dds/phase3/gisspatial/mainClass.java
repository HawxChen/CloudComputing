package org.dds.phase3.gisspatial;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;
import scala.Tuple2;
import org.apache.spark.SparkConf;

/**
 * Hello world! ./bin/spark-submit [spark properties] --class [submission class]
 * [submission jar] [path to input] [path to output]
 */
public class mainClass {
	public static boolean isInteger(String s) {
		boolean isValidInteger = false;
		try {
			Integer.parseInt(s);
			isValidInteger = true;
		} catch (NumberFormatException ex) {
		}
		return isValidInteger;
	}
	
	private static JavaPairRDD<String, Integer> constructPairCell(JavaSparkContext jsc, String[] args) {
		JavaRDD<String> lines = jsc.textFile(args[0]);
		JavaPairRDD<String, Integer> Cells = lines.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<Tuple2<String, Integer>> call(String line) throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<String, Integer>> accu = new ArrayList<Tuple2<String, Integer>>();
				StringTokenizer st = new StringTokenizer(line, ",");
				
				// VendorID,
				if (isInteger(st.nextToken()) == false) {
					accu.add(new Tuple2<String, Integer>("",0));
					return accu.iterator();
				}
				// tpep_pickup_datetime
				final int date = (Integer.parseInt(st.nextToken().split(" ")[0].split("-")[2]) - 1);
				// tpep_dropoff_datetime
				st.nextToken();
				// passenger_count
				st.nextToken();
				// trip_distance
				st.nextToken();
				// pickup_longitude
				String logt_str = st.nextToken();
				// pickup_latitude
				String lat_str = st.nextToken();
				final double logt = Double.parseDouble(logt_str);
				final double lat = Double.parseDouble(lat_str);
				double logt_using;
				double lat_using;
				DecimalFormat df = new DecimalFormat("####");
				df.setRoundingMode(RoundingMode.DOWN);
				for(int date_cnt = -1; date_cnt < 2; date_cnt++) {
					for(int lat_cnt = -1;  lat_cnt < 2; lat_cnt++) {
						for(int logt_cnt = -1; logt_cnt < 2; logt_cnt++) {
							logt_using = (logt + logt_cnt * 0.01 - 0.01)*100;
							lat_using = (lat + lat_cnt*0.01) * 100;
							accu.add(new Tuple2<String, Integer>(df.format(lat_using)+","+ df.format(logt_using) + ","+ Integer.toString(date + date_cnt),1));
						}
					}
				}
				return accu.iterator();
			}

		});
		//Cells.take(50).forEach(tuple2 -> System.out.println(tuple2));
		return Cells;
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		// e.g., latitude 40.5N – 40.9N, longitude 73.7W – 74.25W
		// 40.4N - 41.0N, 73.8W - 74.24W 31-Day
		SparkConf conf = new SparkConf().setAppName("DDSPhase3");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaPairRDD<Integer, String> CellSwap;
		
		CellSwap = constructPairCell(jsc,args).reduceByKey((x, y) -> x+y).mapToPair(tuple -> tuple.swap()).sortByKey(false);
		CellSwap.take(50).forEach(tuple2 -> System.out.println(tuple2._2));

		jsc.stop();

	}

}
