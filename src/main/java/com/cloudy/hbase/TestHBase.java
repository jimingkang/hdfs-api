package com.cloudy.hbase;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;


public class TestHBase {

	/**
	 * @param args
	 */
	public static Configuration configuration ;
	static {
		configuration = HBaseConfiguration.create() ;
//		configuration.set("hbase.master", "master:60000") ;
//		configuration.set("hbase.zookeeper.quorum", "master") ;
		String filePath = "hbase-site.xml" ;
		Path path = new Path(filePath);
		configuration.addResource(path);
	}
	
	public static void CreateTable(String tableName) throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(configuration) ;
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("drop table "+tableName);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		tableDescriptor.addFamily(new HColumnDescriptor("cf1"));
		admin.createTable(tableDescriptor) ;
		
		
		Put put = new Put("123".getBytes()) ;
		put.add("cf1".getBytes(), "colum1".getBytes(), "value1".getBytes()) ;
		put.add("cf1".getBytes(), "colum2".getBytes(), "value2".getBytes()) ;
		put.add("cf1".getBytes(), "colum3".getBytes(), "value3".getBytes()) ;
		
		Put put2 = new Put("234".getBytes()) ;
		put2.add("cf1".getBytes(), "colum1".getBytes(), "value1".getBytes()) ;
		put2.add("cf1".getBytes(), "colum2".getBytes(), "value2".getBytes()) ;
		put2.add("cf1".getBytes(), "colum3".getBytes(), "value3".getBytes()) ;
		
		
		HTable table = new HTable(configuration, tableName) ;
		table.put(put) ;
		table.put(put2) ;
		
	}
	
	//delete row
	public static void deleteRow(String tableName,String rowKey,String rowkey2) throws Exception
	{
		HTable table = new HTable(configuration, tableName) ;
		Delete delete = new Delete(rowKey.getBytes()) ;
		Delete delete2 = new Delete(rowkey2.getBytes()) ;
		List<Delete> list = new ArrayList<Delete>() ;
		list.add(delete);
		list.add(delete2);
		table.delete(list);
		
		
	}
	public static void TestGet(String tableName, String rowKey) throws Exception
	{
		HTable table = new HTable(configuration, tableName) ;
		Get get = new Get(rowKey.getBytes()) ;
		
		Result result = table.get(get);
		System.out.println("test get");
		for(KeyValue keyValue : result.raw())
		{
			System.out.println("cf:"+new String(keyValue.getFamily())+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
		}
		
		
	}
	//select Id,END_USER_ID,ORDER_AMOUNT from so where id between ... and 
	public static void TestScan(String tableName, String startRow, String stopRow) throws Exception
	{
		HTable table = new HTable(configuration, tableName) ;
		Scan scan = new Scan () ;
		scan.addColumn("o".getBytes(), "ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "END_USER_ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "ORDER_AMOUNT".getBytes()) ;
		
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		ResultScanner scanner = table.getScanner(scan) ;
		
		for(Result result : scanner)
		{
			for(KeyValue keyValue : result.raw())
			{
				System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
		}
		
	}
	//select .... from so where end_user_id=?
	public static void Query1(String tableName, String columnValue) throws Exception
	{
		HTable table = new HTable(configuration, tableName) ;
		Scan scan = new Scan () ;
		scan.addColumn("o".getBytes(), "ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "END_USER_ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "ORDER_AMOUNT".getBytes()) ;
		//SingleColumnValueFilter(byte[] family, byte[] qualifier,  compareOp, byte[] value) 
		Filter filter = new SingleColumnValueFilter("o".getBytes(),"END_USER_ID".getBytes(), CompareFilter.CompareOp.EQUAL,columnValue.getBytes());
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan) ;
		
		for(Result result : scanner)
		{
			for(KeyValue keyValue : result.raw())
			{
				System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
			System.out.println();
		}
		
	}
	
	public static void QueryByRowKey(String tableName, String rowKeyRegex) throws Exception
	{
		HTable table = new HTable(configuration, tableName) ;
//		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowKeyRegex)) ;
		PrefixFilter filter = new PrefixFilter(rowKeyRegex.getBytes());
		Scan scan = new Scan();
		scan.addColumn("o".getBytes(), "ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "END_USER_ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "ORDER_AMOUNT".getBytes()) ;
		scan.setFilter(filter);
		
		ResultScanner scanner = table.getScanner(scan);
		int num=0;
		for(Result result : scanner)
		{
			num ++ ;
			
			System.out.println("rowKey:"+new String(result.getRow()));
			for(KeyValue keyValue : result.raw())
			{
				System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
			System.out.println();
		}
		System.out.println(num);
		
	}
	// select ... from so where end_user_id=? and order_status = in (?,?) 
	public static void Query3(String tableName, String end_user_id, String order_status1,String order_status2) throws Exception
	{
		HTable table = new HTable(configuration, tableName) ;
		Scan scan = new Scan();
		scan.addColumn("o".getBytes(), "ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "END_USER_ID".getBytes()) ;
		scan.addColumn("o".getBytes(), "ORDER_STATUS".getBytes()) ;
		
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter("o".getBytes(),"END_USER_ID".getBytes(), CompareFilter.CompareOp.EQUAL,end_user_id.getBytes());
		
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter("o".getBytes(),"ORDER_STATUS".getBytes(), CompareFilter.CompareOp.EQUAL,order_status1.getBytes());
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter("o".getBytes(),"ORDER_STATUS".getBytes(), CompareFilter.CompareOp.EQUAL,order_status2.getBytes());
		
		FilterList filterAll = new FilterList();
		filterAll.addFilter(filter1) ;
		
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		filterList.addFilter(filter2) ;
		filterList.addFilter(filter3) ;
		
		filterAll.addFilter(filterList);
		scan.setFilter(filterAll);
		
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner)
		{
			System.out.println("rowKey:"+new String(result.getRow()));
			for(KeyValue keyValue : result.raw())
			{
				System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
			System.out.println();
		}
		
	}
	// 用普通字段查询
	public static void Query4(String tableName, String so_id) throws Exception
	{
		String rowKeyRegex = StringUtils.reverse(so_id)+"_" ;
		HTable table = new HTable(configuration, tableName) ;
//		RowFilter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator(rowKeyRegex)) ;
		PrefixFilter filter = new PrefixFilter(rowKeyRegex.getBytes());
		Scan scan = new Scan();
		scan.addColumn("d".getBytes(), "ID".getBytes()) ;
		scan.addColumn("d".getBytes(), "ORDER_ID".getBytes()) ;
		scan.addColumn("d".getBytes(), "ORDER_ITEM_AMOUNT".getBytes()) ;
		scan.addColumn("d".getBytes(), "ORDER_ITEM_NUM".getBytes()) ;
		scan.setFilter(filter);
		
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner)
		{
			System.out.println("rowKey:"+new String(result.getRow()));
			for(KeyValue keyValue : result.raw())
			{
				System.out.println(new String(keyValue.getFamily())+":"+new String(keyValue.getQualifier())+"="+new String(keyValue.getValue()));
			}
			System.out.println();
		}
		
	}
	
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
	//	CreateTable("test222") ;
//		deleteRow("test","123","234");
		TestGet("test222","123");
//		TestScan("so","006720521","006720522");
//		Query1("so","123686816");
//		QueryByRowKey("so","00623152");
//		Query3("so","37301766","34","35");
	//	Query4("so_detail","125051400");
	}
	
	

}
