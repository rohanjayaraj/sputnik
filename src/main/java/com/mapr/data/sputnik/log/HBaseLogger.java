package com.mapr.data.sputnik.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.data.sputnik.ext.LoggerStatistics;
import com.mapr.data.sputnik.util.RandomDataGen;

public class HBaseLogger extends Configured implements EventLogger {

	private static final Logger log = LogManager.getLogger(HBaseLogger.class);

	private static final Configuration config = HBaseConfiguration.create();
	private LoggerStatistics stats;
	private final TableName tablename;
	private final String family;
	private final String compression; 
	private final int numsplits;
	private final long keystart;
	private final long keyend;
	private final String keyprefix;
	private byte cfBytes[];
	private final String metaKey = "reps:meta:00001";
	private final String metaFam = "metaFamily";
	private final String metaCol = "lastrow";

	private final boolean bufferwrite;
	private boolean done = false;
	private long msgIdx = 0L;
	private RandomDataGen rdg;
	private final int keyuniqlen = 3;
	private String keyuniq;

	private HTable table = null;
	private HBaseAdmin admin;


	public HBaseLogger(Map<String, Object> props) throws IOException {
		super(config);

		//getConf().set("hbase.zookeeper.quorum", "atsqa4-134.qa.lab,atsqa4-135.qa.lab,atsqa4-136.qa.lab");
		//getConf().set("hbase.zookeeper.property.clientPort", "5181");
		//getConf().set("mapr.hbase.default.db", "maprdb");

		for (Map.Entry<String, String> entry : getConf()) {
			//log.debug(entry.getKey() + " = " + entry.getValue());
		}

		this.tablename =  TableName.valueOf((String)props.get("tablename"));
		this.family =  props.get("family")!=null?(String)props.get("family"):"default";
		this.cfBytes = Bytes.toBytes(this.family);
		this.keyprefix =  props.get("keyprefix")!=null?(String)props.get("keyprefix"):"key";
		this.compression =  props.get("compression")!=null?(String)props.get("compression"):"none";
		this.numsplits =  props.get("numsplits")!=null?(int)props.get("numsplits"):1;
		this.keystart =  props.get("keystart")!=null?(long)props.get("keystart"):1000000;
		this.keyend =  props.get("keyend")!=null?(long)props.get("keyend"):9999999;
		this.stats = new LoggerStatistics();
		
		rdg = RandomDataGen.getInstance();
		this.bufferwrite = props.get("bufferwrite")!=null?(boolean)props.get("bufferwrite"):false;
		keyuniq = rdg.randAlphanum(keyuniqlen, false);

		try {
			if(admin == null){
				admin = new HBaseAdmin(getConf());
			}
			createTable();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			log.error("Exception caught in checking for table. (" + e1.getMessage() + ")");
			e1.printStackTrace();
			return;
		}

	}

	private void createTable() throws IOException{
		try{
			System.out.println(tablename);
			if (admin.tableExists(tablename)) {
				log.info("Table: {} already exists.", tablename);
				Get get = new Get(Bytes.toBytes(metaKey));
				get.addFamily(Bytes.toBytes(metaFam));
				get.addColumn(Bytes.toBytes(metaFam), Bytes.toBytes(metaCol));
				Result r = getHTable().get(get);
				String lastkey = null;
				for (Cell kv : r.rawCells())
				{
					lastkey = Bytes.toString(CellUtil.cloneValue(kv));
					break;

				}
				log.info("Last key added [{}]", lastkey);


				if(lastkey==null){
					log.info("Scanning rows of table {}", tablename);
					Scan scan = new Scan();
					scan.addFamily(Bytes.toBytes(this.family));
					//scan.setReversed(true);
					//scan.setMaxResultSize(100);
					ResultScanner scanner = table.getScanner(scan);

					for (Result rr = scanner.next(); rr != null; rr = scanner.next())
					{
						//get row key
						String key = Bytes.toString(rr.getRow());
						log.info("Key found - {}", key);

					} //done with row
					log.info("Scanning done!");
				}else{
					String[] s = lastkey.split(":");
					if(s.length == 3)
						log.info("Keyprefix : {}, Unique : {} , Index : {}", s[0], s[1], s[2]);
					else
						log.info("lastkey " + s);
					
					if(this.keyprefix.equals(s[0])){
						this.keyuniq = s[1];
						this.msgIdx = Long.parseLong(s[2],16);
					}
				}

				return;
			}
		}catch( Exception e){
			log.error("Exception caught in checking for table. [{}]", e.getMessage());
			e.printStackTrace();
			throw new IOException();
		}

		log.info("Creating table: {} with column family: {} and total {} regions.", tablename, family, numsplits+1);

		HTableDescriptor descriptor = new HTableDescriptor(tablename);
		HColumnDescriptor family = new HColumnDescriptor(this.family);
		family.setCompressionType(Algorithm.NONE);
		descriptor.addFamily(family);
		HColumnDescriptor metafamily = new HColumnDescriptor(this.metaFam);
		metafamily.setCompressionType(Algorithm.NONE);
		descriptor.addFamily(metafamily);
		descriptor.setValue("TABLETYPE", "binary");

		try {
			admin.createTable(descriptor, getSplits());
			log.info("Table: {} created.", tablename);
		} catch (IOException e) {
			log.error("Error creating table: " + tablename + ".\n"
					+ e.getMessage());
			throw e;
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void logEvent(String event) {
		done=true;
		if(!done)
		{
			String tablename = "xyz";
			log.info("******** - > ");

			Configuration config = HBaseConfiguration.create();
			HBaseAdmin a;
			try {
				a = new HBaseAdmin(config);
				//boolean x = a.tableExists();
				//log.info("******** - > " + x);
				HTableDescriptor descriptor = new HTableDescriptor(tablename);
				HColumnDescriptor family = new HColumnDescriptor("abc");
				descriptor.addFamily(family);

				try {
					a.createTable(descriptor);
					log.info("Table: {} created.", tablename);
				} catch (IOException e) {
					log.error("Error creating table: " + tablename + ".\n"
							+ e.getMessage());
					throw e;
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			log.info("<--- ********  ");
			done = true;
		}

		HTable hTable = getHTable();
		if (hTable == null) {
			return;
		}
		if(msgIdx == Long.MAX_VALUE - 1) { msgIdx = 0; keyuniq = rdg.randAlphanum(keyuniqlen, false);}
		String key = keyprefix+":"+keyuniq+":"+String.format("%016x", msgIdx++);
		Put p = new Put(Bytes.toBytes(key));
		p.setDurability(Durability.USE_DEFAULT);

		ObjectMapper mapper = new ObjectMapper();
		try{
			JsonNode node = mapper.readTree(event);
			Iterator<String> fieldNames = node.fieldNames();
			while(fieldNames.hasNext()){
				String fieldName = fieldNames.next();
				JsonNode fieldValue = node.get(fieldName);
				p.add(cfBytes, Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.toString()));
			}
			long start = System.nanoTime();
			hTable.put(p);
			long end = System.nanoTime();
			stats.incrMsgCount();
			stats.addInsertTime(start,end);
			stats.addMsgSize(event.length());
		}catch(Exception e){
			e.printStackTrace();
		}

	}

	private void writeMetaKey(){
		try {
			HTable hTable = getHTable();
			Put p = new Put(Bytes.toBytes(metaKey));
			String key = keyprefix+":"+keyuniq+":"+String.format("%016x", msgIdx-1);
			p.add(Bytes.toBytes(metaFam), Bytes.toBytes(metaCol), Bytes.toBytes(key));
			hTable.put(p);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void shutdown() {
		if (table != null) {
			try {
				writeMetaKey();
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			admin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public HTable getHTable()
	{
		if (table == null)
		{
			synchronized (this) {
				try {
					this.table = new HTable(config, tablename);
					boolean autoFlush = false;
					boolean clearBufferOnFail = false;
					table.setAutoFlush(autoFlush, clearBufferOnFail);

					long writeBufferSize = 12L << 20;
					table.setWriteBufferSize(writeBufferSize);
				} catch (IOException e) {
					System.err.println("Error accessing HBase table: " + e);
					return null;
				}
			}
		}
		return table;
	}

	protected byte[][] getSplits() {
		long delta = (keyend - keystart) / (numsplits);
		List<String> splitKeys = new ArrayList<String>();
		long split = keystart;
		for (int i = 0; i < numsplits; i++) {
			split += delta;
			splitKeys.add(keyprefix + split);
		}
		Collections.sort(splitKeys);
		log.info("Split keys: {}", splitKeys);

		byte[][] splits = new byte[numsplits][];
		int i = 0;
		for (String key : splitKeys) {
			splits[(i++)] = key.getBytes(); 
		}
		return splits;
	}

	@Override
	public LoggerStatistics getStats() {
		// TODO Auto-generated method stub
		return stats;
	}

}
