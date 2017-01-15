package com.jimmy.ibeifeng.hdfs_api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {Configuration con=new Configuration();
    FileSystem fs=null;
    	try {
			 fs= FileSystem.get(con);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println(fs);
    }
}
