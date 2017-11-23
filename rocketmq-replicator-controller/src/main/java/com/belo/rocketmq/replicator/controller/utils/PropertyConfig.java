package com.belo.rocketmq.replicator.controller.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by liqiang on 2017/10/17.
 */
public class PropertyConfig {

    private static final Logger logger = LoggerFactory.getLogger(PropertyConfig.class);

    public static Properties getProperties() {
        return properties;
    }

    private static Properties properties;
    static {
        properties = new Properties();
        InputStream is = PropertyConfig.class.getClassLoader().getResourceAsStream("misc.properties");
        if (is != null) {
            try {
                properties.load(is);
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }



}
