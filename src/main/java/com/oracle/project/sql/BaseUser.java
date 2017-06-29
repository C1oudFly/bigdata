package com.oracle.project.sql;

import java.sql.PreparedStatement;

import org.apache.hadoop.io.IntWritable;

import com.oracle.project.dimention.user.UserDimention;

public interface BaseUser {
	public void setPreparedStatement(PreparedStatement ps,String key,String value);
}
