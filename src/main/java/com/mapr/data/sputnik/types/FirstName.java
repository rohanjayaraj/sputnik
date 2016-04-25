/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik.types;

/**
 *
 * 
 */
public class FirstName extends TypeHandler {
    public static final String TYPE_NAME = "firstName";
    public static final String TYPE_DISPLAY_NAME = "First Name";
    
    private String[] nameList = { "Andrew", "Bob", "Steve", "Sarah", "Tara", "Eric" };

    @Override
    public String getNextRandomValue() {
        return nameList[getRand().randInt(0, nameList.length - 1)];
    }
    
    @Override
    public String getName() {
        return TYPE_NAME;
    }
            
}