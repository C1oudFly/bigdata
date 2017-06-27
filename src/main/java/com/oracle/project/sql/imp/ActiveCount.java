package com.oracle.project.sql.imp;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oracle.project.sql.BaseUser;

public class ActiveCount implements BaseUser {

public void setPreparedStatement(PreparedStatement ps, String key, String value) {
		
		try {
			ps.setString(1, key);
			ps.setString(2, value);
			ps.setString(3, value);
			
			ps.executeUpdate();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
