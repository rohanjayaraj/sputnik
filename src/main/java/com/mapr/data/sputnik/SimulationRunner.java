/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapr.data.sputnik.config.JSONConfigReader;
import com.mapr.data.sputnik.config.SimulationConfig;
import com.mapr.data.sputnik.config.WorkflowConfig;
import com.mapr.data.sputnik.log.EventLogger;
import com.mapr.data.sputnik.workflow.Workflow;

/**
 *
 * 
 */
public class SimulationRunner {

    private static final Logger log = LogManager.getLogger(SimulationRunner.class);
    private SimulationConfig config;
    private List<EventGenerator> eventGenerators;
    private List<Thread> eventGenThreads;
    private boolean running;
    private List<EventLogger> eventLoggers;

    public SimulationRunner(SimulationConfig config, List<EventLogger> loggers) {
        this.config = config;
        this.eventLoggers = loggers;
        eventGenerators = new ArrayList<EventGenerator>();
        eventGenThreads = new ArrayList<Thread>();
        
        setupSimulation();
    }

    private void setupSimulation() {
        running = false;
        for (WorkflowConfig workflowConfig : config.getWorkflows()) {
            try {
                Workflow w = JSONConfigReader.readConfig(this.getClass().getClassLoader().getResourceAsStream(workflowConfig.getWorkflowFilename()), Workflow.class);
                final EventGenerator gen = new EventGenerator(w, workflowConfig.getWorkflowName(), eventLoggers, config.getCommonConfigs());
                log.info("Adding EventGenerator for [ " + workflowConfig.getWorkflowName()+ "," + workflowConfig.getWorkflowFilename()+ " ]");
                eventGenerators.add(gen);
                eventGenThreads.add(new Thread(gen));
            } catch (IOException ex) {
                log.error("Error reading config: " + workflowConfig.getWorkflowName(), ex);
            }
        }
    }

    public void startSimulation() {
        log.info("Starting Simulation");
               
        if (eventGenThreads.size() > 0) {
            for (Thread t : eventGenThreads) {
                t.start();
            }
            running = true;
        }
    }

    public void stopSimulation() {
        log.info("Stopping Simulation");
        for (Thread t : eventGenThreads) {
            t.interrupt();
        }
        for (EventLogger l : eventLoggers) {
            l.shutdown();
        }
        running = false;
    }

    public boolean isRunning() {
        return running;
    }

}
