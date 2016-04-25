/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik.types;

public class EnglishType extends TypeHandler {
    public static final String TYPE_NAME = "english";
    public static final String TYPE_DISPLAY_NAME = "English";
    
    private int length;

    public EnglishType() {
    }
    
    @Override
    public void setLaunchArguments(String[] launchArguments) {
        super.setLaunchArguments(launchArguments);
        if (launchArguments.length != 1) {
            throw new IllegalArgumentException("You must specifc a length for Alpha Numeric types");
        }
        length = Integer.parseInt(launchArguments[0]);
    }

    
    @Override
    public String getNextRandomValue() {
        return getRand().getDictionaryString(length);
    }
            
    @Override
    public String getName() {
        return TYPE_NAME;
    }
}
