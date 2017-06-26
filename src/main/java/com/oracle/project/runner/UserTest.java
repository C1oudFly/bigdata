package com.oracle.project.runner;

import java.sql.Connection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.oracle.project.user.dimention.UserDimention;
import com.oracle.project.user.mapper.UserMapper;
import com.oracle.project.user.outputformat.UserOutputFormat;
import com.oracle.project.user.reducer.UserReducer;

public class UserTest implements Tool {
	private Configuration conf;
	private Connection connection;
	public void setConf(Configuration conf) {
		this.conf = conf;
		conf.addResource("jdbc.xml");
		conf.addResource("project_sql.xml");
	}

	public Configuration getConf() {
		return conf;
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(conf);
		job.setJarByClass(UserTest.class);
		
		job.setMapperClass(UserMapper.class);
		job.setMapOutputKeyClass(UserDimention.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(UserReducer.class);
		job.setOutputKeyClass(UserDimention.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://yunfei1:9000/contextout/part-r-00000"));
		job.setOutputFormatClass(UserOutputFormat.class);
		if(job.waitForCompletion(true)){
			return 1;
		}
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new UserTest(), args);
		System.out.println(result);
	}

}
