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
public class WorkflowConfig {
    private String workflowName;
    private String workflowFilename;

    /**
     * @return the workflowName
     */
    public String getWorkflowName() {
        return workflowName;
    }

    /**
     * @param workflowName the workflowName to set
     */
    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    /**
     * @return the workflowFilename
     */
    public String getWorkflowFilename() {
        return workflowFilename;
    }

    /**
     * @param workflowFilename the workflowFilename to set
     */
    public void setWorkflowFilename(String workflowFilename) {
        this.workflowFilename = workflowFilename;
    }
}
