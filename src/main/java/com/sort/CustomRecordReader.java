package com.sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CustomRecordReader extends Configured implements Tool {

	public static class MyRecordReader extends RecordReader<LongWritable, Text> {

		private long start;
		private long end;
		private long pos;
		private int maxlinelength = 0;
		private LineReader reader;
		private LongWritable key = new LongWritable();
		private FileSystem fs;
		private Path path;
		private FSDataInputStream fileIn;
		private Text value = new Text();

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			reader.close();
			// fileIn.close();
			// fs.close();
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if (start == end)
				return 0.0f;
			else
				return Math.min(1.0f, (pos - start) / (float) (end - start));
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			FileSplit FS = (FileSplit) (split);

			this.path = FS.getPath();

			Configuration conf = context.getConfiguration();

			maxlinelength = conf.getInt(
					"mapreduce.input.linerecordreader.line.maxlength",
					Integer.MAX_VALUE);

			fs = this.path.getFileSystem(conf);

			fileIn = fs.open(path);

			start = FS.getStart();

			end = start + FS.getLength();

			boolean skipFirstLine = false;

			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}

			reader = new LineReader(fileIn, context.getConfiguration());

			if (skipFirstLine) {
				start += reader
						.readLine(value, (int) Math.min(
								(long) Integer.MAX_VALUE, (end - start)));
			}

			this.pos = start;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			key.set(pos);

			int newSize= 0;

			while (pos < end) {

				newSize = reader.readLine(value,
						(int) Math.min((long) Integer.MAX_VALUE, (end - pos)),
						maxlinelength);

				if (newSize == 0) {
					break;
				}

				pos += newSize;

				if (newSize < maxlinelength) {
					break;
				}

				System.out.println("Skipped line of size " + newSize
						+ " at pos " + (pos - newSize));

			}

			if (newSize == 0) {
				// We've reached end of Split
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}

		}

	}

	public static class MyInputFormat extends
			FileInputFormat<LongWritable, Text> {

		@Override
		public RecordReader<LongWritable, Text> createRecordReader(
				InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return new MyRecordReader();
		}

	}

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String args[]) throws Exception {
		int ret = ToolRunner.run(new CustomRecordReader(), args);
		System.exit(ret);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = Job.getInstance(getConf());
		job.setJobName("CustomFileReader");
		job.setJarByClass(CustomRecordReader.class);

		// set the InputFormat of the job to our InputFormat
		job.setInputFormatClass(MyInputFormat.class);

		// the keys are words (strings)
		job.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		job.setOutputValueClass(IntWritable.class);

		// use the defined mapper
		job.setMapperClass(TokenizerMapper.class);
		// use the WordCount Reducer
		job.setReducerClass(IntSumReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
