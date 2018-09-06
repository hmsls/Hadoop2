package MR_wordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyApp_WC {
	public static void main(String[] args) throws Throwable {
		if(args.length != 2){
			System.out.println("usage: <inputPath> <outputPath>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);
		
		job.setJarByClass(MyApp_WC.class);
		job.setJobName("Word Count");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(MyMappre_WC.class);
		job.setReducerClass(MyReducer_WC.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(MyReducer_WC.class);
		
		System.exit(job.waitForCompletion(true)? 1:0);
		
	}
}
