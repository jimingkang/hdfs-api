/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jimmy.ibeifeng.hdfs_api;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class uv extends AbstractJob {




  public static void main(String[] args) throws Exception {

	 
   ToolRunner.run(new uv(), args);
  }

public int run(String[] arg0) throws Exception {
	 Path input=new Path("hdfs://192.168.1.113:9000/user/jimmy/pv/input");
	  Path output=new Path("hdfs://192.168.1.113:9000/user/jimmy/pv/output");
	  JobUtil.delete(super.getConf(), output);
	  Job job=prepareJob(input,
			  output,
			  TextInputFormat.class,
			  UVMapper.class,
			  Text.class,
			  Text.class,
			  UVReducer.class,
			  Text.class,
			  Text.class,
			  TextOutputFormat.class);
	return job.waitForCompletion(true) ? 0 : 1;
}


}
