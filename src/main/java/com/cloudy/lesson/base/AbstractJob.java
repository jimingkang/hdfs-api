
package com.cloudy.lesson.base;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("rawtypes")
public abstract class AbstractJob extends Configured implements Tool {

	private static final Logger log = LoggerFactory
			.getLogger(AbstractJob.class);

	@Override
	public Configuration getConf() {
		Configuration result = super.getConf();
		if (result == null) {
			return new Configuration();
		}
		return result;
	}

	protected Job prepareJob(Path inputPath, Path outputPath,
			Class<? extends InputFormat> inputFormat,
			Class<? extends Mapper> mapper,
			Class<? extends Writable> mapperKey,
			Class<? extends Writable> mapperValue,
			Class<? extends OutputFormat> outputFormat) throws IOException {
		return JobUtil.prepareJob(inputPath, outputPath, inputFormat,
				mapper, mapperKey, mapperValue, outputFormat, getConf());

	}

//	protected Job prepareJob(Path inputPath, Path outputPath,
//			Class<? extends Mapper> mapper,
//			Class<? extends Writable> mapperKey,
//			Class<? extends Writable> mapperValue,
//			Class<? extends Reducer> reducer,
//			Class<? extends Writable> reducerKey,
//			Class<? extends Writable> reducerValue) throws IOException {
//		return prepareJob(inputPath, outputPath, SequenceFileInputFormat.class,
//				mapper, mapperKey, mapperValue, reducer, reducerKey,
//				reducerValue, SequenceFileOutputFormat.class);
//	}

	protected Job prepareJob(Path inputPath, Path outputPath,
			Class<? extends InputFormat> inputFormat,
			Class<? extends Mapper> mapper,
			Class<? extends Writable> mapperKey,
			Class<? extends Writable> mapperValue,
			Class<? extends Reducer> reducer,
			Class<? extends Writable> reducerKey,
			Class<? extends Writable> reducerValue,
			Class<? extends OutputFormat> outputFormat) throws IOException {
		Job job = JobUtil.prepareJob(inputPath, outputPath, inputFormat,
				mapper, mapperKey, mapperValue, reducer, reducerKey,
				reducerValue, outputFormat, getConf());
		return job;
	}

}
