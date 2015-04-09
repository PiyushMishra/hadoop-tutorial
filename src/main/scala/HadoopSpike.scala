
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConversions._
import java.util.StringTokenizer

/**
 * @author piyushm
 */

class TokenizerMapper
  extends Mapper[Object, Text, Text, IntWritable] {

  val one = new IntWritable(1)
  val word = new Text();

  override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context) = {
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

  override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {
    var sum = 0;
    for (v <- values) {
      sum += v.get()
    }
    result.set(sum)
    context.write(key, result)
  }
}

object WordCount {

  def main(args: Array[String]): Int = {
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs
    if (otherArgs.length != 2) {
      println("Usage: wordcount <in> <out>")
      return 2
    }
    val job = new Job(conf, "word count")
    job.setJarByClass(classOf[TokenizerMapper])
    job.setMapperClass(classOf[TokenizerMapper])
    job.setCombinerClass(classOf[IntSumReducer])
    job.setReducerClass(classOf[IntSumReducer])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path((args(1))))
    if (job.waitForCompletion(true)) 0 else 1
  }
}
