package MR_wordCount.copy_inputFormat_DBWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * CombineFileInputFormat,合成文件输入格式
 */
public class MyDBAPP {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: WordCount <input path> <output path>");
			System.exit(-1);
		}
		Job job = Job.getInstance();
		
		//一下所有setXxx都是在设置configuration对象
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(args[1]),true);
		
		job.setJarByClass(MyDBAPP.class);
		job.setJobName("DB Import App");							//设置作业名称
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		//输出路径
		job.setInputFormatClass(DBInputFormat.class);				//设置数据库输入格式
		//设置数据库配置
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://192.168.231.1:3306/test", "root", "root");	
		//设置输入
		DBInputFormat.setInput(job, UserDBWritable.class, "select id,name,age from users", "select count(*) from users");
		
		//mapper类
		job.setMapperClass(DBMapper.class);
		
		//输出keyvalue类
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
