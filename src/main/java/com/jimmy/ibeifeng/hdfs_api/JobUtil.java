/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public final class JobUtil {

  private static final Logger log = LoggerFactory.getLogger(JobUtil.class);

  private JobUtil() { }


public static Job prepareJob(Path inputPath,
                           Path outputPath,
                           Class<? extends InputFormat> inputFormat,
                           Class<? extends Mapper> mapper,
                           Class<? extends Writable> mapperKey,
                           Class<? extends Writable> mapperValue,
                           Class<? extends OutputFormat> outputFormat, Configuration conf) throws IOException {

    Job job = new Job(new Configuration(conf));
    Configuration jobConf = job.getConfiguration();

    if (mapper.equals(Mapper.class)) {
      throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
    }
    job.setJarByClass(mapper);

    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());

    job.setMapperClass(mapper);
    job.setMapOutputKeyClass(mapperKey);
    job.setMapOutputValueClass(mapperValue);
    job.setOutputKeyClass(mapperKey);
    job.setOutputValueClass(mapperValue);
    jobConf.setBoolean("mapred.compress.map.output", true);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }

  /**
   * Create a map and reduce Hadoop job.  Does not set the name on the job.
   * @param inputPath The input {@link org.apache.hadoop.fs.Path}
   * @param outputPath The output {@link org.apache.hadoop.fs.Path}
   * @param inputFormat The {@link org.apache.hadoop.mapreduce.InputFormat}
   * @param mapper The {@link org.apache.hadoop.mapreduce.Mapper} class to use
   * @param mapperKey The {@link org.apache.hadoop.io.Writable} key class.  If the Mapper is a no-op,
   *                  this value may be null
   * @param mapperValue The {@link org.apache.hadoop.io.Writable} value class.  If the Mapper is a no-op,
   *                    this value may be null
   * @param reducer The {@link org.apache.hadoop.mapreduce.Reducer} to use
   * @param reducerKey The reducer key class.
   * @param reducerValue The reducer value class.
   * @param outputFormat The {@link org.apache.hadoop.mapreduce.OutputFormat}.
   * @param conf The {@link org.apache.hadoop.conf.Configuration} to use.
   * @return The {@link org.apache.hadoop.mapreduce.Job}.
   * @throws IOException if there is a problem with the IO.
   *
   * @see #getCustomJobName(String, org.apache.hadoop.mapreduce.JobContext, Class, Class)
   * @see #prepareJob(org.apache.hadoop.fs.Path, org.apache.hadoop.fs.Path, Class, Class, Class, Class, Class,
   * org.apache.hadoop.conf.Configuration)
   */
  public static Job prepareJob(Path inputPath,
                           Path outputPath,
                           Class<? extends InputFormat> inputFormat,
                           Class<? extends Mapper> mapper,
                           Class<? extends Writable> mapperKey,
                           Class<? extends Writable> mapperValue,
                           Class<? extends Reducer> reducer,
                           Class<? extends Writable> reducerKey,
                           Class<? extends Writable> reducerValue,
                           Class<? extends OutputFormat> outputFormat,
                           Configuration conf) throws IOException {

    Job job = new Job(conf);
    Configuration jobConf = job.getConfiguration();

    if (reducer.equals(Reducer.class)) {
      if (mapper.equals(Mapper.class)) {
        throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
      }
      job.setJarByClass(mapper);
    } else {
      job.setJarByClass(reducer);
    }

    job.setInputFormatClass(inputFormat);
    jobConf.set("mapred.input.dir", inputPath.toString());

    job.setMapperClass(mapper);
    if (mapperKey != null) {
      job.setMapOutputKeyClass(mapperKey);
    }
    if (mapperValue != null) {
      job.setMapOutputValueClass(mapperValue);
    }

//    jobConf.setBoolean("mapred.compress.map.output", true);

    job.setReducerClass(reducer);
    job.setOutputKeyClass(reducerKey);
    job.setOutputValueClass(reducerValue);

    job.setOutputFormatClass(outputFormat);
    jobConf.set("mapred.output.dir", outputPath.toString());

    return job;
  }

  public static void delete(Configuration conf, Path path) throws IOException {
    if (conf == null) {
      conf = new Configuration();
    }
      FileSystem fs = path.getFileSystem(conf);
      if (fs.exists(path)) {
        log.info("Deleting {}", path);
        fs.delete(path, true);
      }
  }

}
