package MR_wordCount.copy_inputFormat_keyValue;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMappre_WC extends Mapper<Text,Text,Text,IntWritable>{

	@Override
	protected void map(Text key, Text value,
			Mapper<Text,Text,Text,IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().substring(1).split(" ");
		for(String w:arr){
			context.write(new Text(w), new IntWritable(1));
		}
		context.getCounter("m","mapper.map" + key.toString()).increment(1);
	}
	
}
