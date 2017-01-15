package com.cloudy.lesson.jobs.visit;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudy.lesson.utils.DateUtil;
/**
 * date          2015-09-01
 * province_id   1,2,3...
 * 
 * key: date-province_id
 * 
 * @author hadoop
 *
 */

public class VisitMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String date = null;
	  String url = null;
	  String province_id = null;
	  String guid = null;
	  
	  
    if(value != null)
    {
    	String lines[] = value.toString().split("\\t");
    	if(lines.length>30)
    	{
    		if(lines[1] != null)
    		{
    			url = lines[1] ;
    		}
    		if(lines[5] != null)
    		{
    			guid = lines[5] ;
    		}
    		if(lines[23] != null && lines[23].length()<=2)
    		{
    			province_id = lines[23] ;
    		}
    		if(lines[17] != null && lines[17].length()>8)
    		{
    			date = DateUtil.format(lines[17]) ;   //2015/8/28 19:14:59
    		}
    		
    		context.write(new Text(date+"\t"+province_id), new Text(guid+"_"+url));
    		
    	}
    	
    }
        
  }
}