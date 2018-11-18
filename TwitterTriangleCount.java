import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
	private static final String INTERMEDIATE_OUTPUT_PATH = "intermediate_output";

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

	public static class UndirectedGraphReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
		public reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			Set<LongWritable> uniqueFollowers = HashSet<LongWritable>();

			for (LongWritable value : values) {
				uniqueFollowers.add(value);
			}
			for (LongWritable uniqueFollower : uniqueFollowers) {
				context.write(new Text(key), new Text(uniqueFollower));
			}
		}
	}

	public static class FollowersMapper extends Mapper<Text, Text, LongWritable, LongWritable> {
		public map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
			StringTokenizer keyTokenizer = new StringTokenizer(key.toString());
			StringTokenizer valueTokenizer = new StringTokenizer(value.toString());
			long outKey = Long.parseLong(keyTokenizer.nextToken());
			long outValue = Long.parseLong(valueTokenizer.nextToken());
			context.write(new LongWritable(outKey), new LongWritable(outValue));
		}
	}

	public static class FollowersReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
		public reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			LongWritable[] followerTuple = new LongWritable[2];
			List<LongWritable> followersList = new ArrayList<LongWritable>();

			for (LongWritable followerOne : values) {
				for (LongWritable followerTwo: values) {
					if (followerOne.compareTo(followerTwo) < 0) {
						String outValue = followerOne.get().toString() + "#" + followerTwo.get().toString();
						context.write(new Text(key), new Text(outValue));
					}
				}
			}
		}
	}

	public static class TriangleMapper extends Mapper<Text, Text, Text, Text> {
		public map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String[] splittedValue = value.split("#")
			if (splittedValue.length == 1) {
				// original data
				String newKey = key.toString() + "#" + value.toString();
				context.write(new Text(newKey), "$");
			} else if (splittedValue.length == 2) {
				// two-path data
				context.write(value, key);
			}
		}
	}

	public static class TriangleReducer extends Reducer<Text, Text, Text, LongWritable> {
		public reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			boolean connected = false;
			static long totalTriangles = 0;
			long triangles = 0;
			for (Text value : values) {
				valueString = value.toString();
				if valueString.equals("$") {
					connected = true;
				} else {
					triangles += 1;
				}
			}
			if (connected) {
				totalTriangles += triangles;
			}
			String resultKey = "total_triangles";
			context.write(new Text(resultKey), new LongWritable(totalTriangles));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "GraphUndirecterMapReduceJob");
	    job.setJarByClass(TwitterTriangleCount.class);
	    job.setMapperClass(UndirectedGraphMapper.class);
	    job.setReducerClass(UndirectedGraphReducer.class);
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.waitForCompletion(true);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(INTERMEDIATE_OUTPUT_PATH));

	    Configuration conf2 = new Configuration();
	    Job job2 = Job.getInstance(conf2, "TwoPathMapReduceJob");
	    job2.setJarByClass(TwitterTriangleCount.class);
	    job2.setMapperClass(FollowersMapper.class);
	    job2.setReducerClass(FollowersReducer.class);
	    job2.setMapOutputKeyClass(LongWritable.class);
	    job2.setMapOutputValueClass(LongWritable.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.waitForCompletion(true);

	    FileInputFormat.addInputPath(job2, new Path(INTERMEDIATE_OUTPUT_PATH));
	    FileOutputFormat.setOutputPath(job2, new Path(INTERMEDIATE_OUTPUT_PATH));

	    Configuration conf3 = new Configuration();
	    Job job3 = Job.getInstance(conf3, "TriangleCounterJob");
	    job3.setJarByClass(TwitterTriangleCount.class);
	    job3.setMapperClass(TriangleMapper.class);
	    job3.setReducerClass(TriangleReducer.class);
	    job3.setMapOutputKeyClass(Text.class);
	    job3.setMapOutputValueClass(Text.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(LongWritable.class);

	    FileInputFormat.addInputPath(job3, new Path(INTERMEDIATE_OUTPUT_PATH));
	    FileInputFormat.setOutputPath(job3, new Path(args[1]));

	    System.exit(job3.waitForCompletion(true) ? 0 : 1);
	}
}
