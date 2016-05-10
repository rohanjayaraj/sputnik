/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.mapr.data.sputnik.config;

/**
 *
 * 
 */
public class CommonConfig {
    private boolean parallel;
    private long randseed;
    private int statsInterval;
    private long consolePeriod;
    private String graphitehost;
    private String graphitePortString;
    private long graphitePeriod;
    private String graphitePrefix;
    /**
     * @return the parallel
     */
    public boolean getParallel() {
        return parallel;
    }

    /**
     * @param parallel execution of producers
     */
    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }
    
    /**
     * @return the randseed
     */
    public long getRandseed() {
        return randseed;
    }

    /**
     * @param seed for random data generator
     */
    public void setRandseed(long randseed) {
        this.randseed = randseed;
    }
    
    /**
     * @return the statsInterval
     */
    public int getStatsInterval() {
        return statsInterval;
    }

    /**
     * @param statsInterval to log statistics
     */
    public void setStatsInterval(int statsInterval) {
        this.statsInterval = statsInterval;
    }

	public long getConsolePeriod() {
		return consolePeriod;
	}

	public void setConsolePeriod(long consolePeriod) {
		this.consolePeriod = consolePeriod;
	}

	public String getGraphitehost() {
		return graphitehost;
	}

	public void setGraphitehost(String graphitehost) {
		this.graphitehost = graphitehost;
	}

	public String getGraphitePortString() {
		return graphitePortString;
	}

	public void setGraphitePortString(String graphitePortString) {
		this.graphitePortString = graphitePortString;
	}

	public long getGraphitePeriod() {
		return graphitePeriod;
	}

	public void setGraphitePeriod(long graphitePeriod) {
		this.graphitePeriod = graphitePeriod;
	}

	public String getGraphitePrefix() {
		return graphitePrefix;
	}

	public void setGraphitePrefix(String graphitePrefix) {
		this.graphitePrefix = graphitePrefix;
	}
    
}
