package com.cloudy.kafka;

public interface KafkaProperties
{
  final static String zkConnect = "192.168.1.116:2181";
  final static String broker_list = "192.168.1.116:9092" ;
	final static String hbase_zkList = "192.168.1.116:2181";
	
//  final static String zkConnect   = "192.168.113.80:2181,192.168.113.81:2181,192.168.113.82:2181";
//  final static String broker_list = "192.168.113.80:9092,192.168.113.81:9092,192.168.113.82:9092" ;
//  final static String hbase_zkList = "10.161.164.202,10.161.164.203" ;
  
  final static  String groupId = "group1";
  final static String topic = "log";
  final static String Order_topic = "track_log";
  final static String Log_topic = "log3";
  final static String Order = "order";
//  final static String kafkaServerURL = "localhost";
//  final static int kafkaServerPort = 9092;
//  final static int kafkaProducerBufferSize = 64*1024;
//  final static int connectionTimeOut = 100000;
//  final static int reconnectInterval = 10000;
//  final static String topic2 = "topic2";
//  final static String topic3 = "topic3";
//  final static String clientId = "SimpleConsumerDemoClient";
}
