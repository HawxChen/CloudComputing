/*
 *   ID: 1211181735
 * Name: Ying-Jheng Chen
 */
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {
	public static class ColunmMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokenlist = value.toString().split(",");
			if (tokenlist.length < 2)
				return;

			word.set(tokenlist[1].trim());
			context.write(word, value);
		}
	}

	/*
	 * public static class ConcateStringCombiner extends Reducer<Text, Text,
	 * Text, ArrayWritable> {
	 * 
	 * ArrayList<String> valueArray = new ArrayList<String>(); public void
	 * reduce(Text key, Iterable<Text> values, Context context) throws
	 * IOException, InterruptedException { for (Text value : values) {
	 * valueArray.add(value.toString()); } context.write(key, new ArrayWritable(
	 * valueArray.toArray(new String[valueArray.size()]))); } }
	 */
	public static class ConcateStringReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> valueArrayX = new ArrayList<String>();
			String Xtag = null;
			ArrayList<String> valueArrayY = new ArrayList<String>();
			String Ytag = null;
			String tag = null;

			for (Text value : values) {
				tag = value.toString().split(",")[0].trim();
				if(null == Xtag) {
					Xtag = tag;
				} else if(null == Ytag && false == tag.equals(Xtag)){
					Ytag = tag;
				}
				
				if(tag.equals(Xtag)) {
					valueArrayX.add(0, value.toString());
					
				} else if (tag.equals(Ytag)) {
					valueArrayY.add(0, value.toString());
				}
			}
			
			for (int i_idx = 0; i_idx < valueArrayX.size(); i_idx++) {
				for (int j_idx = 0; j_idx < valueArrayY.size(); j_idx++) {
					context.write(new Text(valueArrayX.get(i_idx) + ", " + valueArrayY.get(j_idx)), null);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "EquiJoin by Mapreduce");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(ColunmMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// job.setCombinerClass(null);
		job.setReducerClass(ConcateStringReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}