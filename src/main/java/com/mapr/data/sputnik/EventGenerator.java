/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.mapr.data.sputnik.config.CommonConfig;
import com.mapr.data.sputnik.log.EventLogger;
import com.mapr.data.sputnik.util.RandomDataGen;
import com.mapr.data.sputnik.workflow.Workflow;
import com.mapr.data.sputnik.workflow.WorkflowStep;

/**
 *
 * 
 */
public class EventGenerator implements Runnable {

    private static final Logger log = LogManager.getLogger(EventGenerator.class);

    private Workflow workflow;
    private String generatorName;
    private boolean running;
    private List<EventLogger> eventLoggers;
    private CommonConfig cconfig;
    private RandomDataGen rdg;
    private long nummsgs = 0;
    private long statsTime = 0;
    private int statsIntervalInSec = 0;

    public EventGenerator(Workflow workflow, String generatorName, List<EventLogger> loggers, CommonConfig cconfig) {
        this.workflow = workflow;
        this.generatorName = generatorName;
        this.eventLoggers = loggers;
        this.cconfig = cconfig;
        this.rdg = RandomDataGen.getInstance(cconfig.getRandseed());
        this.statsIntervalInSec = cconfig.getStatsInterval()==0?30:cconfig.getStatsInterval();
        this.statsTime = System.currentTimeMillis();
    }

    public void runWorkflow() {
        String runMode = "sequential";
        if (workflow.getStepRunMode() != null) {
            runMode = workflow.getStepRunMode();
        }
        switch (runMode) {
            case "sequential":
                runSequential();
                break;
            case "random":
                runRandom();
                break;
            case "random-pick-one":
                runRandomPickOne();
                break;
            default:
                runSequential();
                break;
        }
        printStats(true);
    }

    protected void runSequential() {
        Iterator<WorkflowStep> it = workflow.getSteps().iterator();
        while (running && it.hasNext()) {
            WorkflowStep step = it.next();
            executeStep(step);

            if (!it.hasNext() && workflow.isRepeatWorkflow()) {
                it = workflow.getSteps().iterator();
                try {
                    performWorkflowSleep(workflow);
                } catch (InterruptedException ie) {
                    //wake up!
                    running = false;
                    break;
                }
            }

        }
    }

    protected void runRandom() {
        List<WorkflowStep> stepsCopy = new ArrayList<>(workflow.getSteps());
        Collections.shuffle(stepsCopy, rdg.getRandom());
        
        Iterator<WorkflowStep> it = stepsCopy.iterator();
        while (running && it.hasNext()) {
            WorkflowStep step = it.next();
            executeStep(step);

            if (!it.hasNext() && workflow.isRepeatWorkflow()) {
                Collections.shuffle(stepsCopy, rdg.getRandom());
                it = stepsCopy.iterator();
                try {
                    performWorkflowSleep(workflow);
                } catch (InterruptedException ie) {
                    //wake up!
                    running = false;
                    break;
                }
            }

        }
    }

    protected void runRandomPickOne() {
        while (running) {
            WorkflowStep step = workflow.getSteps().get(rdg.randInt(0, workflow.getSteps().size() - 1));;
            executeStep(step);

            if (workflow.isRepeatWorkflow()) {
                try {
                    performWorkflowSleep(workflow);
                } catch (InterruptedException ie) {
                    //wake up!
                    running = false;
                    break;
                }
            }
        }
    }
    
    protected void printStats(boolean shutdown){
    	long curr = System.currentTimeMillis();
    	if(shutdown || ((curr-statsTime)/1000)>statsIntervalInSec){
    		System.out.println("\nEventLogger Stats [" + generatorName + "]");
    		for (EventLogger l : eventLoggers) {
                System.out.println("\t" + l.getClass().getSimpleName() + "[ #Msgs : " + l.getStats().getNumMsgs() +
                		" , Ave Insert Time (ms) : " + l.getStats().getAverageInsertTime() + 
                		" , Total Time (ms) : " + (l.getStats().getTotalInsertTime()/1000) + 
                		//" , # msg/sec : " + l.getStats().getNumMsgPerSec() + 
                		" , Ave Msg size (bytes) : " + l.getStats().getTotalSize()/l.getStats().getNumMsgs() +
                		" , Total Msg size (MB) : " + l.getStats().getTotalSize()/(1024*1024) +
                		" ] ");
                
            }
    		statsTime = System.currentTimeMillis();
    	}
    }
    
    protected void executeStep(WorkflowStep step) {
        if (step.getDuration() == 0) {
            //Just generate this event and move on to the next one
            for (Map<String, Object> config : step.getConfig()) {
                Map<String, Object> wrapper = new LinkedHashMap<>();
                wrapper.put(null, config);
                try {
                    String event = generateEvent(wrapper);
                    for (EventLogger l : eventLoggers) {
                        l.logEvent(event);
                    }
                    printStats(false);
                    try {
                        performEventSleep(workflow);
                    } catch (InterruptedException ie) {
                        //wake up!
                        running = false;
                        break;
                    }
                } catch (IOException ioe) {
                    log.error("Error generating json event", ioe);
                }
            }
        } else if (step.getDuration() == -1) {
            //Run this step forever
            //They want to continue generating events of this step over a duration
            List<Map<String, Object>> configs = step.getConfig();
            while (running) {
                try {
                    Map<String, Object> wrapper = new LinkedHashMap<>();
                    wrapper.put(null, configs.get(rdg.randInt(0, configs.size() - 1)));
                    String event = generateEvent(wrapper);
                    for (EventLogger l : eventLoggers) {
                        l.logEvent(event);
                    }
                    printStats(false);
                    try {
                        performEventSleep(workflow);
                    } catch (InterruptedException ie) {
                        //wake up!
                        running = false;
                        break;
                    }
                } catch (IOException ioe) {
                    log.error("Error generating json event", ioe);
                }
            }
        } else {
            //They want to continue generating events of this step over a duration
            long now = new Date().getTime();
            long stopTime = now + step.getDuration();
            List<Map<String, Object>> configs = step.getConfig();
            while (new Date().getTime() < stopTime && running) {
                try {
                    Map<String, Object> wrapper = new LinkedHashMap<>();
                    wrapper.put(null, configs.get(rdg.randInt(0, configs.size() - 1)));
                    String event = generateEvent(wrapper);
                    for (EventLogger l : eventLoggers) {
                        l.logEvent(event);
                    }
                    printStats(false);
                    try {
                        performEventSleep(workflow);
                    } catch (InterruptedException ie) {
                        //wake up!
                        running = false;
                        break;
                    }
                } catch (IOException ioe) {
                    log.error("Error generating json event", ioe);
                }
            }
        }
    }

    

    private void performEventSleep(Workflow workflow) throws InterruptedException {
        long durationBetweenEvents = workflow.getEventFrequency();
        if (workflow.isVaryEventFrequency()) {
            long minSleep = durationBetweenEvents - durationBetweenEvents / 2;
            long maxSleep = durationBetweenEvents;
            Thread.sleep(rdg.randLong(minSleep, maxSleep));
        } else {
            Thread.sleep(durationBetweenEvents);
        }
    }

    private void performWorkflowSleep(Workflow workflow) throws InterruptedException {
        if (workflow.getTimeBetweenRepeat() > 0) {
            if (workflow.isVaryRepeatFrequency()) {
                long sleepDur = workflow.getTimeBetweenRepeat();
                long minSleep = sleepDur - sleepDur / 2;
                long maxSleep = sleepDur;
                Thread.sleep(rdg.randLong(minSleep, maxSleep));
            } else {
                Thread.sleep(workflow.getTimeBetweenRepeat());
            }
        }
    }

    public String generateEvent(Map<String, Object> config) throws IOException {
    	nummsgs++;
        RandomJsonGenerator generator = new RandomJsonGenerator(config);
        return generator.generateJson();
    }

    public void run() {
        try {
            setRunning(true);
            runWorkflow();
            setRunning(false);
        } catch (Throwable ie) {
            log.fatal("Exception occured causing the Generator to shutdown", ie);
            setRunning(false);
        }
    }

    /**
     * @return the running
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * @param running the running to set
     */
    public void setRunning(boolean running) {
        this.running = running;
    }

}
