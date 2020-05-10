import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONObject;

public class RedditAverage extends Configured implements Tool {

	public static class MapperReddit
	extends Mapper<LongWritable, Text, Text, LongPairWritable>{

		private Text word = new Text();
		private final static LongPairWritable pair = new LongPairWritable();
		private final static LongWritable one = new LongWritable(1);

		//@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
			
			JSONObject record = new JSONObject(value.toString());
			word.set((String) record.get("subreddit"));
			pair.set(one.get(),(Integer) record.get("score"));
			context.write(word, pair);

			}
		}

	public static class CombinerReddit
	extends Reducer<Text, LongPairWritable, Text, LongPairWritable>{

		private final static LongPairWritable pair = new LongPairWritable();
		private Text word = new Text();

		//@Override
		public void combine(Text key, Iterable<LongPairWritable> values, Context context
				) throws IOException, InterruptedException {
			long count = 0;
			long sum = 0;
			for (LongPairWritable val : values) {
				count += val.get_0();
				sum += val.get_1();
			}
			pair.set(count,sum);
			context.write(key, pair);
			}
		}

	public static class ReducerReddit
	extends Reducer<Text, LongPairWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		//@Override
		public void reduce(Text key, Iterable<LongPairWritable> values,
				Context context
				) throws IOException, InterruptedException {
			double count = 0;
			double sum = 0;
			for (LongPairWritable val : values) {
				count += val.get_0();
				sum += val.get_1();
			}
			result.set((Double) sum/count);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "reddit average");
		job.setJarByClass(RedditAverage.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(MapperReddit.class);
		job.setCombinerClass(CombinerReddit.class);
		job.setReducerClass(ReducerReddit.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongPairWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
