package com.cloudy.mapred.jobs.visit;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudy.mapred.base.AbstractJob;
import com.cloudy.mapred.base.JobUtil;

public final class VisitJob extends AbstractJob {

	private static final Logger LOG = LoggerFactory.getLogger(VisitJob.class);


	public static void main(String[] args) throws Exception {
		ToolRunner.run(new VisitJob(), args);
	}


	  public int run(String[] args) throws Exception {
		LOG.info("Program start on "+this.getClass().getName());
		Path input = new Path("hdfs://192.168.1.113:9000/user/jimmy/");
		Path output = new Path("hdfs://192.168.1.113:9000/user/jimmy/output_window");
		JobUtil.delete(super.getConf(), output);
		
		Job job = prepareJob(input,
					output,
					TextInputFormat.class,
					VisitMapper.class,
					Text.class,
					Text.class,
					VisitReducer.class,
					Text.class,
					Text.class,
					TextOutputFormat.class);
		
		
		boolean succeeded = job.waitForCompletion(true);
	    if (!succeeded) {
	      return -1;
	    }
	    return 0;
	}


	

}
