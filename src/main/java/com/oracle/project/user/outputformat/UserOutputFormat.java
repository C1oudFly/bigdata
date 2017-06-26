package com.oracle.project.user.outputformat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oracle.project.connect.JdbcManager;
import com.oracle.project.sql.BaseUser;
import com.oracle.project.sql.imp.AddUser;
import com.oracle.project.sql.imp.VisitCount;
import com.oracle.project.sql.imp.VisitUser;
import com.oracle.project.user.dimention.UserDimention;

public class UserOutputFormat extends OutputFormat<UserDimention, IntWritable> {

	@Override
	public RecordWriter<UserDimention, IntWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	class MysqlRecordWriter extends RecordWriter<UserDimention, IntWritable>{
		private int total = 0;
		private int visitCount = 0;
		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		
		private HashMap<String, PreparedStatement> pSMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, Integer> addMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> visitMap = new LinkedHashMap();
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}

		int count = 0;
		@Override
		public void write(UserDimention key, IntWritable value) throws IOException, InterruptedException {
			ps = pSMap.get(key.getEn());
			
			if(ps == null){
				try {
					ps = connection.prepareStatement(configuration.get(key.getEn()));
					pSMap.put(key.getEn(), ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(key.getEn().equals("adduser")){
				addMap.put(key.getDate(), Integer.parseInt(value.toString()));
				
			}else{
				
				visitCount = visitMap.get(key.getDate()) == null ? 0 : visitMap.get(key.getDate()) + 1;
				visitMap.put(key.getDate(), visitCount);
				
			}
		
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		
			for(String date : addMap.keySet()){
				new AddUser().setPreparedStatement(pSMap.get("adduser"), date, addMap.get(date));
			}
			
			
			
			for(String date : visitMap.keySet()){
				new VisitUser().setPreparedStatement(pSMap.get("visituser"), date, visitMap.get(date));
			}
			
			try {
				pSMap.put("selectlast", connection.prepareStatement(configuration.get("selectlast")));
				pSMap.put("visitcount", connection.prepareStatement(configuration.get("visitcount")));
				
				PreparedStatement ps = pSMap.get("selectlast");
				ResultSet rs = ps.executeQuery();
				
				
				while(rs.next()){
					total = Integer.parseInt(rs.getString(2));
				}
				
				for(String date : visitMap.keySet()){
					
					if(total == 0){
						total += visitMap.get(date);
					}
					else {
						total += visitMap.get(date);
					}
					
					new VisitCount().setPreparedStatement(pSMap.get("visitcount"), date, total);
				}
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for(String s : pSMap.keySet()){
				PreparedStatement ps = pSMap.get(s);
				try {
					ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
	}

}
