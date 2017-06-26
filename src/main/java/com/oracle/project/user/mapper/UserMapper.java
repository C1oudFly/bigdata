package com.oracle.project.user.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.oracle.project.user.dimention.UserDimention;
import com.oracle.project.utils.ToDate;

public class UserMapper extends Mapper<LongWritable, Text, UserDimention, IntWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, UserDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String lines [] = value.toString().split("\t");
		//==================================新增用户==============================
		String date =ToDate.toDate(lines[1]);
		String url = lines[3];
		
		if(url.contains("en=e_l")){
			UserDimention userDimention = new UserDimention();
			userDimention.setDate(date);
			userDimention.setEn("adduser");
			userDimention.setU_uid("");
			context.write(userDimention, new IntWritable(1));
		}
		
		//==================================活跃用户==============================
		String urls[] = url.split("&");
		String u_ud [] = null;
		for (String str : urls) {
			if(str.contains("u_ud")){
				u_ud = str.split("=");
				
				UserDimention userDimention = new UserDimention();
				userDimention.setDate(date);
				userDimention.setEn("visituser");
				userDimention.setU_uid(u_ud[1]);
				context.write(userDimention, new IntWritable(0));
				
				break;
			}
		}
		
	}
}
