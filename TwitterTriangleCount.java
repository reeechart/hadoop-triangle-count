import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TwitterTriangleCount {
	private static final String INTERMEDIATE_OUTPUT_PATH_1 = "intermediate_output_one";

	public static class UndirectedGraphMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			LongWritable mapKey = new LongWritable();
			LongWritable mapValue = new LongWritable();
			StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
			String userOne = stringTokenizer.nextToken();
			String userTwo = stringTokenizer.nextToken();
			long userOneID = Long.parseLong(userOne);
			long userTwoID = Long.parseLong(userTwo);

			if (userOneID < userTwoID) {
				// emit(userOneID, userTwoID)
				emited.add(toBeEmited);
				mapKey.set(userOneID);
				mapValue.set(userTwoID);
				context.write(mapKey, mapValue);
			} else if (userOneID > userTwoID) {
				// emit(userTwoID, userOneID)
				emited.add(toBeEmited);
				mapKey.set(userTwoID);
				mapValue.set(userOneID);
				context.write(mapKey, mapValue);
			}
		}
	}

	public static class UndirectedGraphReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
		public reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Set<LongWritable> uniqueFollowers = HashSet<LongWritable>();
			for (LongWritable value : values) {
				uniqueFollowers.add(value);
			}
			for (LongWritable uniqueFollower : uniqueFollowers) {
				context.write(key, uniqueFollower);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

	    Job job = Job.getInstance(conf, "twitter_triangle_count");
	    job.setJarByClass(TwitterTriangleCount.class);
	    job.setMapperClass(UndirectedGraphMapper.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(LongWritable.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
