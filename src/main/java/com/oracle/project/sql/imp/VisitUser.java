package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.io.IntWritable;

import com.oracle.project.sql.BaseUser;
import com.oracle.project.user.dimention.UserDimention;

public class VisitUser implements BaseUser {

	public void setPreparedStatement(PreparedStatement ps, String  key, Integer value) {
		
		try {
			ps.setString(1, key);
			ps.setString(2, value.toString());
			ps.setString(3, value.toString());
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
