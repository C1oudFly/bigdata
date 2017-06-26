package com.oracle.project.runner;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Test {

	public static void main(String[] args) {
		
		String s = "[19/Jun/2017:08:17:34 +0800]";
		
		
		System.out.println(toDateH(s));
		
		System.out.println(toDate(toDateH(s)));
	}
	
	
	public static String toDateH(String date){
		SimpleDateFormat in = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss ZZZZZ]", Locale.US);
		SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd:HH");
		
		try {
			Date date1 = in.parse(date);
			date = out.format(date1);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date;
	}
	
	public static String toDate(String date){
		SimpleDateFormat in = new SimpleDateFormat("yyyy-MM-dd:HH");
		SimpleDateFormat out = new SimpleDateFormat("yyyy-MM-dd");
		
		try {
			Date date1 = in.parse(date);
			date = out.format(date1);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return date;
	}

}
