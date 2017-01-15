package com.jimmy.ibeifeng.hdfs_api;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgTimeUVReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();
	HashMap<Text, HashSet> hp = new HashMap<Text, HashSet>();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		String arr[] =null;
		HashSet hs=null;
		int pv=0;
		for (Text val : values) {
			 arr= val.toString().split("\\t");
			if (hp.get(key) != null) {
				if(!hs.contains(arr[0]))
               hs.add(arr[0]);
			} else {
				hs=new HashSet();
				hs.add(arr[0]);
			}
			hp.put(key, hs);
			pv++;
		}
		result.set(hp.get(key).size()+"\t"+pv);
		context.write(key, result);
	}
}