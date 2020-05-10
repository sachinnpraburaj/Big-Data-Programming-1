import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WikipediaPopular extends Configured implements Tool {

	public static class WikiMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private Text time = new Text();
		private final static IntWritable count = new IntWritable();

		//@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
      String[] line = value.toString().split(" ");
      String title = line[2];

      if(!(title.equals("Main_Page")))
      {
        if(!title.startsWith("Special:"))
        {
					if(line[1].equals("en"))
          {
              time.set(line[0]);
              count.set(Integer.parseInt(line[3]));
              context.write(time,count);
          }
				}
			}
    }
	}

	public static class WikiCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable max_count = new IntWritable();

		//@Override
		public void combine(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int maxvalue = 0;
			for(IntWritable val : values)
			{
				if(val.get() > maxvalue)
				{
						maxvalue = val.get();
				}
			}
			max_count.set(maxvalue);
			context.write(key, max_count);
		}
	}

	public static class WikiReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		//@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int maxvalue = 0;
			for(IntWritable val : values)
			{
		    if(val.get() > maxvalue)
				{
		        maxvalue = val.get();
		    }
			}
			result.set(maxvalue);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "wikipedia popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(WikiMapper.class);
		job.setCombinerClass(WikiCombiner.class);
		job.setReducerClass(WikiReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
