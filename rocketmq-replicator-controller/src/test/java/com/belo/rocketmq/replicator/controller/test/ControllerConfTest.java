package com.belo.rocketmq.replicator.controller.test;

import com.belo.rocketmq.replicator.controller.ControllerConf;
import org.junit.Test;

/**
 * Created by liqiang on 2017/10/19.
 */
public class ControllerConfTest {

    @Test
    public void testConfFromHaihe(){
        try {
            ControllerConf controllerConf = ControllerConf.getControllerConf();
            System.out.println(controllerConf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
