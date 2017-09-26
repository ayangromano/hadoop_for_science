/*Kmer-counting code used by developers of Contrail*/
*
*http://www.homolog.us/blogs/blog/2011/08/31/using-hadoop-for-transcriptomics-an-example-to-get-started/
*
*Attempted on old installation of Hadoop in Google CLoud*/
*
*Failed on compile: javac -classpath ../hadoop-0.20.2/hadoop-0.20.2-core.jar:../hadoop-0.20.2/lib/commons-cli-1.2.jar -d kmer_classes Kmer.java
*
*Compile also attempted on newer Hadoop: javac -classpath ../hadoop-1.2.1/hadoop-core-1.2.1.jar:../hadoop-1.2.1/lib/commons-cli-1.2.jar -d kmer_classes Kmer.java
*
*/

package us.homolog;

import java.io.IOException;
import java.lang.InterruptedException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Kmer
{
public static class KmerMapper extends Mapper
{
private final static IntWritable one = new IntWritable(1);
private Text word = new Text();

public void map(Object key, Text value, Context context)
throws IOException, InterruptedException
{
String line=value.toString();
String sub;
for(int i=0; i<=line.length()-10; i++) { sub=line.substring(i,i+10); word.set(sub); context.write(word, one); } } } public static class KmerReducer extends Reducer
{
public void reduce(Text key, Iterable values, Context context)
throws IOException, InterruptedException
{
int sum = 0;
for (IntWritable value : values)
{
sum += value.get();
}
context.write(key, new IntWritable(sum));
}
}

public static void main(String[] args) throws Exception
{
Configuration conf = new Configuration();
String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
Job job = new Job(conf, "Example Hadoop 0.20.1 Kmer");
job.setJarByClass(Kmer.class);
job.setMapperClass(KmerMapper.class);
job.setReducerClass(KmerReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}