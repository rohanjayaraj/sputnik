/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mapr.data.sputnik.types;

import java.util.Date;

/**
 *
 * 
 */
public class DateType extends BaseDateType {

    public static final String TYPE_NAME = "date";
    public static final String TYPE_DISPLAY_NAME = "Date";

    public DateType() {
    }
    
    @Override
    public Date getNextRandomValue() {
        return getRandomDate();
    }
    
    @Override
    public String getName() {
        return TYPE_NAME;
    }
}
