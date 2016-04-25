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
public class BooleanType extends TypeHandler {
    public static final String TYPE_NAME = "boolean";
    public static final String TYPE_DISPLAY_NAME = "Boolean";

    @Override
    public Boolean getNextRandomValue() {
        return getRand().randBoolean();
    }
    
    @Override
    public String getName() {
        return TYPE_NAME;
    }
            
}
