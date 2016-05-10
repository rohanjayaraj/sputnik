package com.mapr.data.sputnik.ext;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.base.Strings;
import com.mapr.data.sputnik.config.CommonConfig;

public class StatsReporter {
	private static StatsReporter instance = null;
	
	final private MetricRegistry registry;
	private long consolePeriod = 30;
	private String graphitehost;
	private String graphitePortString;
	private long graphitePeriod = 30;
	private String graphitePrefix;
	
	public static StatsReporter getInstance() {
		return instance;
	}
	
	public synchronized static void createInstance(CommonConfig config) {
		if (instance == null) {
			instance = new StatsReporter(config);
		}
	}
	
	private StatsReporter(CommonConfig config){
		this.registry = new MetricRegistry();
		this.consolePeriod = config.getConsolePeriod();
		this.graphitehost = config.getGraphitehost();
		this.graphitePortString = config.getGraphitePortString();
		this.graphitePeriod = config.getGraphitePeriod();
		this.graphitePrefix = config.getGraphitePrefix();
		startReporters();
	}

	private void startReporters(){
		if (consolePeriod > 0) {
			ConsoleReporter.forRegistry(registry).convertRatesTo(TimeUnit.SECONDS)
			.convertDurationsTo(TimeUnit.MILLISECONDS).build().start(consolePeriod, TimeUnit.SECONDS);
		}

		if (!Strings.isNullOrEmpty(graphitehost) && !Strings.isNullOrEmpty(graphitePortString) && graphitePeriod > 0) {
			Graphite graphite = new Graphite(new InetSocketAddress(graphitehost, Integer.parseInt(graphitePortString)));
			GraphiteReporter reporter = GraphiteReporter.forRegistry(registry).prefixedWith(graphitePrefix)
					.convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL).build(graphite);
			reporter.start(graphitePeriod, TimeUnit.SECONDS);
		}
	}
	
	public MetricRegistry getRegistry(){
		return registry;
	}
}
