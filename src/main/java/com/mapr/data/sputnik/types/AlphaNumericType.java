/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik.types;

public class AlphaNumericType extends TypeHandler {
    public static final String TYPE_NAME = "alphaNumeric";
    public static final String TYPE_DISPLAY_NAME = "Alpha Numeric";
    
    private int min = -1;
    private int max;

    public AlphaNumericType() {
    }

    @Override
    public void setLaunchArguments(String[] launchArguments) {
        super.setLaunchArguments(launchArguments);
        if (launchArguments.length != 1) {
            throw new IllegalArgumentException("You must specifc a length for Alpha Numeric types");
        }else if (launchArguments.length == 2){
        	min = Integer.parseInt(launchArguments[0]);
        	max = Integer.parseInt(launchArguments[1]);
        	min=min>max?max:min;
        }else 
        	max = Integer.parseInt(launchArguments[0]);
        min = min==-1?max:min;
    }
    
    @Override
    public String getNextRandomValue() {
        return getRand().createRandomString(max);
    }

    @Override
    public String getName() {
        return TYPE_NAME;
    }
            
}
