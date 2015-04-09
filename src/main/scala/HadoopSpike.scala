
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author piyushm
 */

class WordCount {

  class TokenizerMapper
    extends Mapper[Object, Text, Text, IntWritable] {

    private val one = new IntWritable(1)
    private val word = new Text();

    def map(key: Object, value: Text, context: Context) = {
      val itr = new StringTokenizer(value.toString())
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken())
        context.write(word, one)
      }
    }
  }

  class IntSumReducer
    extends Reducer[Text, IntWritable, Text, IntWritable] {
    private val result = new IntWritable()

    def reduce(key: Text, values: Iterable[IntWritable],
               context: Context) {
      var sum = 0;
      for (v <- values) {
        sum += v.get()
      }
      result.set(sum)
      context.write(key, result)
    }
  }

  def main(args: Array[String]) {    
    val conf = new Configuration();
    val job = Job.getInstance(conf, "word count");
    job.setJarByClass(classOf[WordCount]);
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    println(args(0), args(2))
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))
    val value = if (job.waitForCompletion(true)) 0 else 1
    System.exit(value)
  }
}
