package com.cloudy.lesson.jobs.visit;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VisitReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
	}
	
	HashMap<Text,HashSet> map = new HashMap<Text,HashSet>();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		int pv = 0;
        String arr[] = null;
        HashSet<String> set =  null;
        
		for (Text value : values) {
			if(value != null)
			{
				arr=value.toString().split("_") ;
				if(map.get(key) != null)
				{
					if(! set.contains(key))
					{
						set.add(arr[0]);
					}
				}else
				{
					set = new HashSet<String>();
					set.add(arr[0]);
				}
				map.put(key, set) ;
				pv ++ ;
			}
		}
		
		context.write(key, new Text(map.get(key).size()+"\t"+pv));
	}
}