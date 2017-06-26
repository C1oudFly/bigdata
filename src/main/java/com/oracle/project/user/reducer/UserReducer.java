package com.oracle.project.user.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.oracle.project.user.dimention.UserDimention;

public class UserReducer extends Reducer<UserDimention, IntWritable, UserDimention, IntWritable> {
	
	@Override
	protected void reduce(UserDimention key, Iterable<IntWritable> value,
			Reducer<UserDimention, IntWritable, UserDimention, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for(IntWritable i : value){
			count += i.get();
		}
		context.write(key, new IntWritable(count));
	}

}
