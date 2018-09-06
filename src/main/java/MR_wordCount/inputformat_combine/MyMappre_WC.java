package MR_wordCount.inputformat_combine;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMappre_WC extends Mapper<NullWritable,BytesWritable,Text,IntWritable>{

	protected void map(NullWritable key, BytesWritable value,
			Mapper<LongWritable,Text,Text,IntWritable>.Context context)
			throws IOException, InterruptedException {
		Text text = new Text();
		IntWritable one = new IntWritable(1);
		
		byte[] data = value.copyBytes();
		String str = new String(data,"utf-8");
		
		String[] arr = str.replaceAll("\r\n", " ").split(" ");
		for(String w:arr){
			text.set(w);
			context.write(text, one);
		}
	}
	
}
