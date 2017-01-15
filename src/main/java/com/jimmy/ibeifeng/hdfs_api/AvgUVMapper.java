package com.jimmy.ibeifeng.hdfs_api;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgUVMapper extends Mapper<Object, Text, Text, Text> {

	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] ss = value.toString().split("\\t");
		String date=null;
		String province=null;
		String guid=null;
		if (value != null) {
			if (ss.length > 30) {
				if(ss[17]!=null&&ss[17].length()>8)
				date=ss[17];
				if(ss[23]!=null&&ss[23].length()<=2)
				province=ss[23];
				if(ss[5]!=null)
					guid=ss[5];
				word.set(date + "_" + province);
				Text res = new Text();
				res.set(guid+"_"+ss[1]);
				context.write(word, res);
			}
		}
	}
}
