/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik.log;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.mapr.data.sputnik.ext.LoggerStatistics;
import com.mapr.data.sputnik.ext.StatsReporter;
import com.mapr.data.sputnik.util.JsonUtils;

/**
 *
 * 
 */
public class KafkaLogger implements EventLogger {

    private static final Logger log = LogManager.getLogger(KafkaLogger.class);
    
    public static final String BROKER_SERVER_PROP_NAME = "broker.server";
    public static final String BROKER_PORT_PROP_NAME = "broker.port";
    
    private long msgIdx = 0;
    private int partition = -1;
    
    private final KafkaProducer<String, String> producer;
    private final List<PartitionInfo> pInfo;
    private final String topic;
    private final String key;
    private final boolean sync;
    private final boolean flatten;
    private final Properties props = new Properties();
    private JsonUtils jsonUtils;
    private LoggerStatistics stats;
    
    private final Timer logTime;
	private final Counter logCount;
	private final Histogram logSize;
    
    public KafkaLogger(Map<String, Object> props) {
        String brokerHost = (String) props.get(BROKER_SERVER_PROP_NAME);
        Integer brokerPort = (Integer) props.get(BROKER_PORT_PROP_NAME);
        
        this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerHost + ":" + brokerPort.toString());
        this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        
        producer = new KafkaProducer<String, String>(this.props);
        
        this.topic = (String) props.get("topic");
        pInfo = producer.partitionsFor(this.topic);
        
        this.key = props.get("key_prefix")!=null?(String)props.get("key_prefix"):"key";
        String p = (String)props.get("partition");
        if(p!=null){
        	try{
	        	int pv = Integer.valueOf(p);
	        	if(pv >= 0 && pv < pInfo.size()) 
	        		this.partition = pv;
        	}catch(java.lang.NumberFormatException e){}
        }
        this.sync = props.get("sync")!=null?(Boolean) props.get("sync"):false;
        this.flatten = props.get("flatten")!=null?(Boolean) props.get("flatten"):false;
        this.jsonUtils = new JsonUtils();
        this.stats = new LoggerStatistics();
        
        this.logTime = StatsReporter.getInstance().getRegistry().timer(MetricRegistry.name(KafkaLogger.class.getSimpleName(), "inserttime"));
		this.logCount = StatsReporter.getInstance().getRegistry().counter(MetricRegistry.name(KafkaLogger.class.getSimpleName(), "msgcount"));
		this.logSize = StatsReporter.getInstance().getRegistry().histogram(MetricRegistry.name(KafkaLogger.class.getSimpleName(), "rowsize"));
    }

    @Override
    public void logEvent(String event) {
        String output = event;
        if (flatten) {
            try {
                output = jsonUtils.flattenJson(event);
            } catch (IOException ex) {
                log.error("Error flattening json. Unable to send event [ " + event + " ]", ex);
                return;
            }
        }
        
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
        		topic, (partition!=-1?partition:(int)(msgIdx%pInfo.size())), (key+msgIdx++), output);
        long start = System.currentTimeMillis();
        final Timer.Context context = logTime.time();
		if (sync) {
            try {
                producer.send(producerRecord).get();
            } catch (InterruptedException | ExecutionException ex) {
                //got interrupted while waiting
                log.warn("Thread interrupted while waiting for synchronous response from producer", ex);
            }
        } else {
            producer.send(producerRecord);
        }
		context.close();
		logCount.inc();
		logSize.update(event.length());
		long end = System.currentTimeMillis();
		stats.incrMsgCount();
		stats.addInsertTime(start,end);
		stats.addMsgSize(event.length());
    }

    @Override
    public void shutdown() {
        producer.close();
    }

	@Override
	public LoggerStatistics getStats() {
		// TODO Auto-generated method stub
		return stats;
	}

}
