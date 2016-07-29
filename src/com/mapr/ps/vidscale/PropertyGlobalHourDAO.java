package com.mapr.ps.vidscale;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


public class PropertyGlobalHourDAO {

	//public static final String userdirectory =System.getProperty("user.home");
		public static final String userdirectory = "/tables";
		public static final String tableName = userdirectory + "/property_global_hour";
		// Schema variables for ShoppingCart Table
		public static final byte[] TABLE_NAME = Bytes.toBytes(tableName);
		// column family: items
		public static final byte[] CF = Bytes.toBytes("cf");
		// 
		// columns: uniq_vis, bytes,
		public static byte[] UNIQ_VIS_COL = Bytes.toBytes("uniq_vis");
		public static byte[] BYTES_COL = Bytes.toBytes("bytes");

		private static final Logger log = Logger.getLogger(PropertyGlobalHourDAO.class);
		private HTableInterface table = null;

		public PropertyGlobalHourDAO(HTableInterface tableInterface) {
			this.table = tableInterface;
		}

		public PropertyGlobalHourDAO(Configuration conf) throws IOException{
			this.table = new HTable(conf, TABLE_NAME);
		}


		public Put mkPutUniqueVis(String rowKey, int quantity) {
			log.debug(String.format("Creating Put for %s", rowKey));
			Put p = new Put(Bytes.toBytes(rowKey)); //
			p.addColumn(CF, UNIQ_VIS_COL, Bytes.toBytes(quantity));
			System.out.println("mkPut   [rowkey=" + rowKey + ", quantity= "
					+ quantity + "]");
			return p;
		}
		
		public Put mkPutBytes(String rowKey, double quantity) {
			log.debug(String.format("Creating Put for %s", rowKey));
			Put p = new Put(Bytes.toBytes(rowKey)); //
			p.addColumn(CF, BYTES_COL, Bytes.toBytes(quantity));
			System.out.println("mkPut   [rowkey=" + rowKey + ", quantity= "
					+ quantity + "]");
			return p;
		}

		public Put mkPut(String rowKey, byte[] fam, byte[] qual, byte[] val) {
			Put p = new Put(Bytes.toBytes(rowKey));
			p.addColumn(fam, qual, val);
			return p;
		}
		
		public Put mkPutVersion(String rowKey, byte[] fam, byte[] qual, long version, long quantity) {
			log.debug(String.format("Creating Put for %s", rowKey));
			Put p = new Put(Bytes.toBytes(rowKey), version); //
			p.addColumn(fam, qual, Bytes.toBytes(quantity));
			System.out.println("mkPut   [rowkey=" + rowKey + ", quantity= "
					+ quantity + "]");
			return p;
		}
		
		public void putBatch(List<Put> puts) throws Exception {

			Object[] results = new Object[puts.size()];
			table.batch(puts, results);
			for (Object result : results) {
				System.out.println("batch put result: " + result);
			}
		
		}
	    
		public void close() throws IOException {
			table.close();
		}
}

