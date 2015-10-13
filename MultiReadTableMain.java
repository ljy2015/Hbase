package com.datacenter.HbaseMapReduce.MultiReadTable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.datacenter.HbaseMapReduce.Read.ReadHbase;
import com.datacenter.HbaseMapReduce.Read.ReadHbaseMapper;

public class MultiReadTableMain {
	static public String rootdir = "hdfs://hadoop3:8020/hbase";
	static public String zkServer = "hadoop3";
	static public String port = "2181";

	private static Configuration conf;
	private static HConnection hConn = null;

	public static HConnection HbaseUtil(String rootDir, String zkServer, String port) {

		conf = HBaseConfiguration.create();// 获取默认配置信息
		conf.set("hbase.rootdir", rootDir);
		conf.set("hbase.zookeeper.quorum", zkServer);
		conf.set("hbase.zookeeper.property.clientPort", port);

		try {
			hConn = HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hConn;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		HbaseUtil(rootdir, zkServer, port);

		// Configuration config = HBaseConfiguration.create();

		Job job = new Job(conf, "ExampleRead");
		job.setJarByClass(ReadHbase.class); // class that contains mapper

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad for
								// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob("score", // input HBase table name
				scan, // Scan instance to control CF and attribute selection
				MuliTableReadmapper.class, // mapper
				null, // mapper output key
				null, // mapper output value
				job);
		job.setOutputFormatClass(NullOutputFormat.class); // because we aren't
															// emitting anything
															// from mapper

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}

}
