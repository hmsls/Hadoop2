package MR_wordCount.inputformat_combine;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer_WC extends Reducer<Text,IntWritable,Text,IntWritable>{

	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text,IntWritable,Text,IntWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		
		for(IntWritable i:value){
			count++;
		}
		context.write(key, new IntWritable(count));
	}
	
}
