package MR_wordCount.copy_inputFormat_DBWritable;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DBMapper extends Mapper<LongWritable, UserDBWritable, Text, Text> {

	protected void map(LongWritable key, UserDBWritable value,
			Mapper<LongWritable, UserDBWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		int id = value.getId();
		String name = value.getName();
		int age = value.getAge();
		//i tom,12
		context.write(new Text("" + id), new Text(name + "," + age));
	}
}

