package com.cloudy.mapred.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * 
 * Description: 日期时间工具类 <br/>
 * 
 */
public final class DateUtil {

	private static DateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd");
	private static DateFormat fmt1 = new SimpleDateFormat("yyyy-MM-dd");
	
	public static String format(String dateString){
		try {
			return datetimeFormat.format(fmt1.parse(dateString));
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}
			
	}

	public static void main(String[] args) {
		System.out.println(DateUtil.format("2015-8-28 19:14:59"));
	}
	
}



