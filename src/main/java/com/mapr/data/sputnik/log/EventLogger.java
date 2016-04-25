/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.mapr.data.sputnik.log;

import com.mapr.data.sputnik.ext.LoggerStatistics;

/**
 *
 * 
 */
public interface EventLogger {
    public void logEvent(String event);
    public void shutdown();
    public LoggerStatistics getStats();
}
