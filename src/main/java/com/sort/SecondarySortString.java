package com.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SecondarySortString {

	public static class CompositeKey implements
			WritableComparable<CompositeKey> {

		String firstname = "";
		String lastname = "";

		public void set(String firstname, String lastname) {
			this.firstname = firstname;
			this.lastname = lastname;
		}

		public String getFirstName() {
			return this.firstname;
		}

		public String getLastName() {
			return this.firstname;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			firstname = in.readUTF();
			lastname = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeUTF(firstname);
			out.writeUTF(lastname);
		}

		@Override
		public int compareTo(CompositeKey key) {
			// TODO Auto-generated method stub
			if (this.firstname != key.firstname) {
				return -1;
			} else if (this.lastname != key.lastname) {
				return 1;
			} else {
				return 0;
			}
		}

		@Override
		public boolean equals(Object key) {
			CompositeKey compositeKey = (CompositeKey) key;
			return (this.firstname == compositeKey.firstname && this.lastname == compositeKey.lastname);
		}
	}

	/** A Comparator that compares serialized CompositeKey. */
	public static class Comparator extends WritableComparator {
		public Comparator() {
			super(CompositeKey.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(CompositeKey.class, new Comparator());
	}

	public static class FirstPartitioner extends
			Partitioner<CompositeKey, String> {
		@Override
		public int getPartition(CompositeKey key, String value,
				int numReduceTasks) {
			// TODO Auto-generated method stub
			return key.getFirstName().hashCode() % numReduceTasks;
		}
	}

	// RawComparator
	public static class FirstGroupingPartitioner implements
			RawComparator<CompositeKey>

	{

		@Override
		public int compare(CompositeKey arg0, CompositeKey arg1) {
			// TODO Auto-generated method stub

			if (arg0.firstname != arg1.firstname) {
				return -1;
			} else if (arg0.lastname != arg1.lastname) {
				return 1;
			} else {
				return 0;
			}
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int first1 = WritableComparator.readInt(b1, s1);
			int first2 = WritableComparator.readInt(b2, s2);
			return first1 < first2 ? -1 : first1 == first2 ? 0 : 1;
		}
	}

	// map reduce part

	public static class Map extends Mapper<LongWritable, Text, CompositeKey, Text> {

		@Override
		public void map(LongWritable arg0, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub

			String firstname = "";
			String lastname = "";

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			if (tokenizer.hasMoreTokens()) {

				firstname = tokenizer.nextToken();
				if (tokenizer.hasMoreTokens()) {
					lastname = tokenizer.nextToken();
				}
			}

			CompositeKey key = new CompositeKey();
			key.set(firstname, lastname);
			value.set(lastname);
			context.write(key, value);

		}

	}

	public static class Reduce extends Reducer<CompositeKey, Text, Text, Text> {
		private static final Text SEPARATER = new Text(
				"---------------------------------------------");
		private static final Text firstnameText = new Text();

		public void reduce(CompositeKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			String firstname = key.getFirstName();
			
			firstnameText.set(firstname);

			for (Text value : values) {
				context.write(firstnameText, value);
				context.write(SEPARATER, null);
			}

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: secondarysort <in> <out>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "secondarysortstring");

		job.setJarByClass(SecondarySortString.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingPartitioner.class);

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
