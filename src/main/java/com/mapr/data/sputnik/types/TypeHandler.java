/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik.types;

import com.mapr.data.sputnik.util.RandomDataGen;

/**
 *
 * 
 */
public abstract class TypeHandler {
    private RandomDataGen rand;
    private String[] launchArguments;
    
    public TypeHandler() {
        rand = RandomDataGen.getInstance();
    }
    
    public abstract Object getNextRandomValue();
    public abstract String getName();
    
    /**
     * @return the rand
     */
    public RandomDataGen getRand() {
        return rand;
    }

    /**
     * @param rand the rand to set
     */
    public void setRand(RandomDataGen rand) {
        this.rand = rand;
    }

    /**
     * @return the launchArguments
     */
    public String[] getLaunchArguments() {
        return launchArguments;
    }

    /**
     * @param launchArguments the launchArguments to set
     */
    public void setLaunchArguments(String[] launchArguments) {
        this.launchArguments = launchArguments;
    }
    
    public static String stripQuotes(String s) {
        return s.replaceAll("'", "").replaceAll("\"", "").trim();
    }
}
