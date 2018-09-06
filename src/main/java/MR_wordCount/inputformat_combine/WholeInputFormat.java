package MR_wordCount.inputformat_combine;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
/**
 * 自定义输入的格式，将整个文件作为一条record
 * @author LISHUAI
 *
 */
public class WholeInputFormat extends FileInputFormat<NullWritable,BytesWritable> {

	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		WhileRecordReader reader = new WhileRecordReader();
		reader.initialize(split, context);
		context.getCounter("in","wholeInputFormat.whileReader.new").increment(1);
		return reader;
	}
	
}
