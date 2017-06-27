package com.oracle.project.user.outputformat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.oracle.project.connect.JdbcManager;
import com.oracle.project.sql.imp.ActiveMember;
import com.oracle.project.sql.imp.AddMember;
import com.oracle.project.sql.imp.AddUser;
import com.oracle.project.sql.imp.AverageSessionLength;
import com.oracle.project.sql.imp.SessionLength;
import com.oracle.project.sql.imp.SessionNumber;
import com.oracle.project.sql.imp.VisitUser;
import com.oracle.project.user.dimention.UserDimention;
import com.oracle.project.utils.Pshandler;

public class UserOutputFormat extends OutputFormat<UserDimention, LongWritable> {

	@Override
	public RecordWriter<UserDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	class MysqlRecordWriter extends RecordWriter<UserDimention, LongWritable>{
		private int visitCount = 0;
		private int memberCount = 0;
		private int activeCount = 0;
		private int sessionnumberCount = 0;
		private double sessionlengthCount = 0;
		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, Integer> adduserMap = new HashMap<String, Integer>();
		private HashMap<String, Integer> visituserMap = new LinkedHashMap();
		
		private HashMap<String, Integer> addmemberMap = new LinkedHashMap();
		private HashMap<String, Integer> activememberMap = new LinkedHashMap();
		
		private HashMap<String, Integer> sessionnumberMap = new LinkedHashMap();
		private HashMap<String, Double> sessionlengthMap = new LinkedHashMap();
		
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}

		int count = 0;
		@Override
		public void write(UserDimention key, LongWritable value) throws IOException, InterruptedException {
			ps = psMap.get(key.getSign());
			
			if(ps == null){
				try {
					ps = connection.prepareStatement(configuration.get(key.getSign()));
					psMap.put(key.getSign(), ps);
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if(key.getSign().equals("adduser")){
				adduserMap.put(key.getDate(), Integer.parseInt(value.toString()));
				
			}else if(key.getSign().equals("visituser")){
				visitCount = (visituserMap.get(key.getDate()) == null ? 0 : visituserMap.get(key.getDate())) + 1;
				visituserMap.put(key.getDate(), visitCount);
			}else if (key.getSign().equals("addmember")) {
				memberCount = (addmemberMap.get(key.getDate()) == null ? 0 : addmemberMap.get(key.getDate())) + 1;
				addmemberMap.put(key.getDate(), memberCount);
				
			}else if (key.getSign().equals("activemember")) {
				activeCount = (activememberMap.get(key.getDate()) == null ? 0 : activememberMap.get(key.getDate())) + 1;
				activememberMap.put(key.getDate(), activeCount);
			}else if (key.getSign().equals("sessionnumber")) {
				sessionnumberCount = (sessionnumberMap.get(key.getDate()) == null ? 0 : sessionnumberMap.get(key.getDate())) + 1;
				sessionnumberMap.put(key.getDate(), sessionnumberCount);
			}else if (key.getSign().equals("sessionlength")) {
				sessionlengthCount = (sessionlengthMap.get(key.getDate()) == null ? Integer.parseInt(value.toString()) : sessionlengthMap.get(key.getDate())) + Integer.parseInt(value.toString());
				sessionlengthMap.put(key.getDate(), sessionlengthCount);
			}
		
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			try {
				psMap.put("slvisitcount", connection.prepareStatement(configuration.get("slvisitcount")));
				psMap.put("visitcount", connection.prepareStatement(configuration.get("visitcount")));
				psMap.put("slactivecount", connection.prepareStatement(configuration.get("slactivecount")));
				psMap.put("activecount", connection.prepareStatement(configuration.get("activecount")));
				psMap.put("averagesessionlength", connection.prepareStatement(configuration.get("averagesessionlength")));
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			for(String date : adduserMap.keySet()){
				new AddUser().setPreparedStatement(psMap.get("adduser"), date, adduserMap.get(date).toString());
			}
			
			for(String date : visituserMap.keySet()){
				new VisitUser().setPreparedStatement(psMap.get("visituser"), date, visituserMap.get(date).toString());
			}
			
			for(String date : addmemberMap.keySet()){
				new AddMember().setPreparedStatement(psMap.get("addmember"), date, addmemberMap.get(date).toString());
			}
			
			for(String date : activememberMap.keySet()){
				new ActiveMember().setPreparedStatement(psMap.get("activemember"), date, activememberMap.get(date).toString());
			}
			
			for(String date : sessionnumberMap.keySet()){
				new SessionNumber().setPreparedStatement(psMap.get("sessionnumber"), date, sessionnumberMap.get(date).toString());
			}
			
			for(String date : sessionlengthMap.keySet()){
				double d = sessionlengthMap.get(date);
				d = d / 60000;
				sessionlengthMap.put(date, d);
				new SessionLength().setPreparedStatement(psMap.get("sessionlength"), date, String.format("%.2f", d));
			}
			
			for(String date : sessionlengthMap.keySet()){
				double d = 0;
				d = sessionlengthMap.get(date) / sessionnumberMap.get(date);
				new AverageSessionLength().setPreparedStatement(psMap.get("averagesessionlength"), date, String.format("%.2f", d));
			}
			
			new Pshandler().psHandler(psMap,visituserMap,"slvisitcount","visitcount",configuration.get("VisitCount"));
			new Pshandler().psHandler(psMap, addmemberMap, "slactivecount", "activecount", configuration.get("ActiveCount"));
			
			for(String s : psMap.keySet()){
				PreparedStatement ps = psMap.get(s);
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
