package com.wyd.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {
    static Properties properties;

    static {
        properties = new Properties();

        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("userDraw.properties");
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static final String Separator = properties.getProperty("Separator");

    public static final String Date = properties.getProperty("Date");

    public static final String MDN = properties.getProperty("MDN");

    public static final String appId = properties.getProperty("appId");

    public static final String count = properties.getProperty("count");

    public static final String ProcedureTime = properties.getProperty("ProcedureTime");
}
