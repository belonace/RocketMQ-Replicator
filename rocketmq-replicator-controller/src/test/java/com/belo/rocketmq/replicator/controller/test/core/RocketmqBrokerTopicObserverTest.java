package com.belo.rocketmq.replicator.controller.test.core;

import com.belo.rocketmq.replicator.controller.core.RocketmqBrokerTopicObserver;
import org.junit.Test;

/**
 * 主要测试对应的RocketmqBrokerTopicObserver
 * Created by liqiang on 2017/10/26.
 */
public class RocketmqBrokerTopicObserverTest {

    @Test
    public void rocketmqBrokerTopicObserverTest() {

        RocketmqBrokerTopicObserver rocketmqBrokerTopicObserver =
                new RocketmqBrokerTopicObserver("demo", "10.241.11.211:9876");

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(rocketmqBrokerTopicObserver.getAllTopics());
        System.out.println(rocketmqBrokerTopicObserver.getNumTopics());
        System.out.println(rocketmqBrokerTopicObserver.getTopicPartition("rr_demo2"));

    }
}
