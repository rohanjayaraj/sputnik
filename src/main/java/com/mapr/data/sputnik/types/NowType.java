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
public class NowType extends NowBaseType {

    public static final String TYPE_NAME = "now";
    public static final String TYPE_DISPLAY_NAME = "Now";

    @Override
    public Date getNextRandomValue() {
        return getNextDate();
    }

    @Override
    public String getName() {
        return TYPE_NAME;
    }
}
