package com.mapr.ps.vidscale;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;


public class TestApp {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
		//Here assuming that the table already exists, if no create the table
		PropertyGlobalHourDAO dao = new PropertyGlobalHourDAO(conf);
		List<Put> puts = new ArrayList<Put>();
		System.out.println("------------------------------");
		System.out.println(" Inserting rows in Table: ");
		//1 row
		puts.add(dao.mkPutUniqueVis("1562838400:131077:1",  9));
		puts.add(dao.mkPutBytes("1562838400:131077:1",  40.33));
		
		//2 row
		puts.add(dao.mkPutUniqueVis("1562838400:131077:5",  11));
		puts.add(dao.mkPutBytes("1562838400:131077:5",  32.32));
		
		//3 row
		puts.add(dao.mkPutUniqueVis("1562838400:131077:8",  6));
		puts.add(dao.mkPutBytes("1562838400:131077:8",  11.81));
		
		// put data in table as a batch
		dao.putBatch(puts);

	}

}
