package com.datacenter.HbaseMapReduce.MultiReadTable;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.datacenter.HbaseMapReduce.Read.ReadHbase;

public class MuliTableReadmapper extends TableMapper<Text, LongWritable> {

	private ResultScanner rs=null; 
	
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		printResult(value);
		
		//输出第二张表的内容
		Result temp=rs.next();//这个结果只是一个单元的结果，所谓一个单元可以理解成是一行的数据
		while(temp!=null){
			printResult(temp);
			temp=rs.next();
		}
		
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		HConnection hconn = MultiReadTableMain.HbaseUtil(
				MultiReadTableMain.rootdir, MultiReadTableMain.zkServer,
				MultiReadTableMain.port);

		HTableInterface ht = hconn.getTable("test");

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs

		rs = ht.getScanner(scan);
		
	}

	// 按顺序输出
	public void printResult(Result rs) {

		if (rs.isEmpty()) {
			System.out.println("result is empty!");
			return;
		}

		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> temps = rs
				.getMap();
		String rowkey = Bytes.toString(rs.getRow()); // actain rowkey
		System.out.println("rowkey->" + rowkey);
		for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> temp : temps
				.entrySet()) {
			System.out.print("\tfamily->" + Bytes.toString(temp.getKey()));
			for (Entry<byte[], NavigableMap<Long, byte[]>> value : temp
					.getValue().entrySet()) {
				System.out.print("\tcol->" + Bytes.toString(value.getKey()));
				for (Entry<Long, byte[]> va : value.getValue().entrySet()) {
					System.out.print("\tvesion->" + va.getKey());
					System.out.print("\tvalue->"
							+ Bytes.toString(va.getValue()));
					System.out.println();
				}
			}
		}
	}

}
