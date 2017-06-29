package com.oracle.project.outputformat.browser;

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
import com.oracle.project.dimention.browser.BrowserDimention;
import com.oracle.project.sql.imp.ActiveMemberBrowser;
import com.oracle.project.sql.imp.ActiveUserBrowser;
import com.oracle.project.sql.imp.AddMemberBrowser;
import com.oracle.project.sql.imp.AddUserBrowser;
import com.oracle.project.sql.imp.PvBrowser;
import com.oracle.project.sql.imp.SessionLengthBrowser;
import com.oracle.project.sql.imp.SessionNumberBrowser;

public class OutputFormatBrowser extends OutputFormat<BrowserDimention, LongWritable> {

	@Override
	public RecordWriter<BrowserDimention, LongWritable> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		Connection connection = JdbcManager.getConnection(conf);
		
		return new MysqlRecordWriter(conf,connection);
	}
	
	public class MysqlRecordWriter extends RecordWriter<BrowserDimention, LongWritable>{
		private PreparedStatement ps;
		private Configuration configuration;
		private Connection connection;
		private String newkey;
		private HashMap<String, PreparedStatement> psMap = new HashMap<String, PreparedStatement>();
		private HashMap<String, Integer> adduserMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, Integer> activeuserMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, Integer> userMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, Integer> addmemberMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, Integer> activememberMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, Integer> memberMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, Integer> sessionnumberMap = new LinkedHashMap<String, Integer>();
		private HashMap<String, String> sessionlengthMap = new LinkedHashMap<String, String>();
		private HashMap<String, Integer> browserpvMap = new LinkedHashMap<String, Integer>();
		
		public MysqlRecordWriter(Configuration conf, Connection connection) {
			this.configuration = conf;
			this.connection = connection;
		}

		int count = 0;
		int dvalue = 0;
		@Override
		public void write(BrowserDimention key, LongWritable value) throws IOException, InterruptedException {
			newkey = key.getDate() + "\t" + key.getB_iev();
			if(key.getSign().equals("adduser")){
				
				if(adduserMap.get(newkey) == null){
					count = (int) value.get();
				}else {
					count = (int) (adduserMap.get(newkey) + value.get());
				}
				adduserMap.put(newkey, count);
			}else if (key.getSign().equals("activeuser")) {
				if(activeuserMap.get(newkey) == null){
					count = 1;
				}else {
					count = activeuserMap.get(newkey) + 1;
				}
				
				activeuserMap.put(newkey, count);
			}else if (key.getSign().equals("addmember")) {
				if(addmemberMap.get(newkey) == null){
					count = 1;
				}else {
					count = addmemberMap.get(newkey) + 1;
				}
				
				addmemberMap.put(newkey, count);
			}else if (key.getSign().equals("activemember")) {
				if(activememberMap.get(newkey) == null){
					count = 1;
				}else {
					count = activememberMap.get(newkey) + 1;
				}
				
				activememberMap.put(newkey, count);
			}else if (key.getSign().equals("sessionnumber")) {
				if(sessionnumberMap.get(newkey) == null){
					count = 1;
				}else {
					count = sessionnumberMap.get(newkey) + 1;
				}
				
				sessionnumberMap.put(newkey, count);
			}else if (key.getSign().equals("sessionlength")) {
				if(sessionlengthMap.get(newkey) == null){
					count = 1;
					dvalue = (int) value.get();
				}else {
					count = Integer.parseInt(sessionlengthMap.get(newkey).split("\t")[1]) + 1;
					dvalue = (int) (Integer.parseInt(sessionlengthMap.get(newkey).split("\t")[0]) + value.get());
				}
				
				sessionlengthMap.put(newkey, dvalue + "\t" + count);
			}else if (key.getSign().equals("browserPV")) {
				if(browserpvMap.get(newkey) == null){
					count = (int) value.get();
				}else {
					count = (int) (browserpvMap.get(newkey) + value.get());
				}
				
				browserpvMap.put(newkey, count);
			}
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			
			try {
				psMap.put("adduserbrowser", connection.prepareStatement(configuration.get("adduserbrowser")));
				psMap.put("activeuserbrowser", connection.prepareStatement(configuration.get("activeuserbrowser")));
				psMap.put("addmemberbrowser", connection.prepareStatement(configuration.get("addmemberbrowser")));
				psMap.put("activememberbrowser", connection.prepareStatement(configuration.get("activememberbrowser")));
				psMap.put("sessionnumberbrowser", connection.prepareStatement(configuration.get("sessionnumberbrowser")));
				psMap.put("sessionlengthbrowser", connection.prepareStatement(configuration.get("sessionlengthbrowser")));
				psMap.put("pvbrowser", connection.prepareStatement(configuration.get("pvbrowser")));
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			for(String s : adduserMap.keySet()){
				new AddUserBrowser().setPreparedStatement(psMap.get("adduserbrowser"), s, Integer.toString(adduserMap.get(s)));
			}
			
			for(String s : activeuserMap.keySet()){
				new ActiveUserBrowser().setPreparedStatement(psMap.get("activeuserbrowser"), s, Integer.toString(activeuserMap.get(s)),Integer.toString(0));
			}
			
			String news;
			for(String s : activeuserMap.keySet()){
				news = s.split("\t")[1];
				userMap.put(news, (userMap.get(news) == null ? 0 : userMap.get(news)) + activeuserMap.get(s));
				new ActiveUserBrowser().setPreparedStatement(psMap.get("activeuserbrowser"), s, Integer.toString(activeuserMap.get(s)),Integer.toString(userMap.get(news)));

			}
			
			for(String s : addmemberMap.keySet()){
				new AddMemberBrowser().setPreparedStatement(psMap.get("addmemberbrowser"), s, Integer.toString(addmemberMap.get(s)),Integer.toString(0));
			}
			
			for(String s : activememberMap.keySet()){
				new ActiveMemberBrowser().setPreparedStatement(psMap.get("activememberbrowser"), s, Integer.toString(activememberMap.get(s)));
			}
			
			for(String s : addmemberMap.keySet()){
				news = s.split("\t")[1];
				memberMap.put(news, (memberMap.get(news) == null ? 0 : memberMap.get(news)) + addmemberMap.get(s));
				new AddMemberBrowser().setPreparedStatement(psMap.get("addmemberbrowser"), s, Integer.toString(addmemberMap.get(s)),Integer.toString(memberMap.get(news)));

			}
			
			for(String s : sessionnumberMap.keySet()){
				new SessionNumberBrowser().setPreparedStatement(psMap.get("sessionnumberbrowser"), s, Integer.toString(sessionnumberMap.get(s)),Integer.toString(0));
			}
			
			for(String s : sessionlengthMap.keySet()){
				new SessionLengthBrowser().setPreparedStatement(psMap.get("sessionlengthbrowser"), s, sessionlengthMap.get(s));
			}
			
			double average = 0;
			String values[];
			for(String s : sessionlengthMap.keySet()){
				values = sessionlengthMap.get(s).split("\t");
				dvalue = Integer.parseInt(values[0]);
				count = Integer.parseInt(values[1]);
				average = dvalue / count;
				new SessionNumberBrowser().setPreparedStatement(psMap.get("sessionnumberbrowser"), s, Integer.toString(sessionnumberMap.get(s)),String.format("%.2f", average)); 
			}
			
			for(String s : browserpvMap.keySet()){
				new PvBrowser().setPreparedStatement(psMap.get("pvbrowser"), s, Integer.toString(browserpvMap.get(s)));
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
