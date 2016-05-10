package com.mapr.data.sputnik.log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ojai.Document;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.mapr.data.sputnik.ext.LoggerStatistics;
import com.mapr.data.sputnik.ext.StatsReporter;
import com.mapr.data.sputnik.util.RandomDataGen;
import com.mapr.db.Admin;
import com.mapr.db.FamilyDescriptor;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import com.mapr.db.Table.TableOption;
import com.mapr.db.exceptions.DBException;
import com.mapr.db.exceptions.TableExistsException;

public class JsonDBLogger implements EventLogger {

	private static final Logger log = LogManager.getLogger(JsonDBLogger.class);

	private final String tablename;
	private final String family;
	private final boolean insertOrder; 
	private final int numsplits;
	private final long keystart;
	private final long keyend;
	private final String keyprefix;

	private final boolean bufferwrite;
	private long msgIdx = 0L;
	private RandomDataGen rdg;
	private String keyuniq;
	private LoggerStatistics stats;

	private volatile Table table = null;
	
	private final Timer logTime;
	private final Counter logCount;
	private final Histogram logSize;

	public JsonDBLogger(Map<String, Object> props) {
		this.tablename =  (String)props.get("tablename");
		this.family =  props.get("family")!=null?(String)props.get("family"):"default";
		this.keyprefix =  props.get("keyprefix")!=null?(String)props.get("keyprefix"):"key";
		this.insertOrder =  props.get("insertOrder")!=null?(boolean)props.get("insertOrder"):true;
		this.numsplits =  props.get("numsplits")!=null?(int)props.get("numsplits"):1;
		this.keystart =  props.get("keystart")!=null?(long)props.get("keystart"):1000000;
		this.keyend =  props.get("keyend")!=null?(long)props.get("keyend"):9999999;
		createTable();
		rdg = RandomDataGen.getInstance();
		this.bufferwrite = props.get("bufferwrite")!=null?(boolean)props.get("bufferwrite"):false;
		keyuniq = rdg.randAlphanum(3, false);
		this.stats = new LoggerStatistics();
		
		this.logTime = StatsReporter.getInstance().getRegistry().timer(MetricRegistry.name(JsonDBLogger.class.getSimpleName(), "inserttime"));
		this.logCount = StatsReporter.getInstance().getRegistry().counter(MetricRegistry.name(JsonDBLogger.class.getSimpleName(), "msgcount"));
		this.logSize = StatsReporter.getInstance().getRegistry().histogram(MetricRegistry.name(JsonDBLogger.class.getSimpleName(), "rowsize"));

	}

	private void createTable(){
		if ( MapRDB.tableExists(tablename)) {
			log.info("Table: {} already exists.", tablename);
			return;
		}

		log.info("Creating table: {} with column family: {} and total {} regions.", tablename, family, numsplits+1);

		try {
			Admin admin = MapRDB.newAdmin();
			TableDescriptor tableDescriptor = MapRDB.newTableDescriptor()
					.setPath(tablename)
					.setInsertionOrder(insertOrder);
			FamilyDescriptor familyDesc = MapRDB.newDefaultFamilyDescriptor()
					.setCompression(FamilyDescriptor.Compression.None);
			tableDescriptor.addFamily(familyDesc);

			admin.createTable(tableDescriptor, getSplits());

			log.info("Table: {} created successfully", tablename);
		}catch (TableExistsException e) {
			log.error("Error creating table: " + tablename + ".\n"
					+ e.getMessage());
			e.printStackTrace();
		} catch (DBException e) {
			log.error("Error creating table: " + tablename + ".\n"
					+ e.getMessage());
			e.printStackTrace();
		}
	}
	@Override
	public void logEvent(String event) {
		// TODO Auto-generated method stub
		try {
			Table table = getTable(tablename);
			Document document = (Document) MapRDB.newDocument(event);
			if(msgIdx == Long.MAX_VALUE - 1) { msgIdx = 0; keyuniq = rdg.randAlphanum(3, false);}
			String key = keyprefix+":"+keyuniq+":"+String.format("%016x", msgIdx++);
			document.setId(key);
			long start = System.currentTimeMillis();
			final Timer.Context context = logTime.time();
			table.insertOrReplace(document);
			context.close();
			logCount.inc();
			logSize.update(event.length());
			long end = System.currentTimeMillis();
			stats.incrMsgCount();
			stats.addInsertTime(start,end);
			stats.addMsgSize(event.length());
		} catch (DBException | IOException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
	}

	private Table getTable(String tableName) throws DBException, IOException {
		if (table == null) {
			synchronized (this) {
				if (table == null) {
					table = MapRDB.getTable(tableName);
					table.setOption(TableOption.EXCLUDEID, true);
					table.setOption(TableOption.BUFFERWRITE, bufferwrite);
				}
			}
		}
		return table;
	}

	@Override
	public void shutdown() {
		if (table != null) {
			try {
				table.close();
			} catch (Exception e) {
				throw new DBException(e);
			}
		}

	}

	protected String[] getSplits() {
		long delta = (keyend - keystart) / (numsplits);
		List<String> splitKeys = new ArrayList<String>();
		long split = keystart;
		for (int i = 0; i < numsplits; i++) {
			split += delta;
			splitKeys.add(keyprefix + split);
		}
		Collections.sort(splitKeys);
		log.info("Split keys: {}", splitKeys);

		String[] splits = new String[numsplits];
		int i = 0;
		for (String key : splitKeys) {
			splits[(i++)] = key;
		}
		return splits;
	}

	@Override
	public LoggerStatistics getStats() {
		// TODO Auto-generated method stub
		return stats;
	}

}
