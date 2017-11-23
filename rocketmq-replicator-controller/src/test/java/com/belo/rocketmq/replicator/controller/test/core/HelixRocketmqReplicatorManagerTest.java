package com.belo.rocketmq.replicator.controller.test.core;

import com.belo.rocketmq.replicator.controller.ControllerConf;
import com.belo.rocketmq.replicator.controller.core.AutoTopicWhitelistingManager;
import com.belo.rocketmq.replicator.controller.core.HelixRocketmqReplicatorManager;
import com.belo.rocketmq.replicator.controller.core.RocketmqBrokerTopicObserver;
import com.belo.rocketmq.replicator.controller.reporter.HelixRocketMQReplicatorMetricsReporter;
import org.junit.Test;

/**
 * 主要测试两个类的行为
 * HelixRocketmqReplicatorManager
 * 测试这个类需要环境全部搭建起来
 * Created by liqiang on 2017/10/26.
 */
public class HelixRocketmqReplicatorManagerTest {

    @Test
    public void test() {
        ControllerConf controllerConf = null;
        try {
            controllerConf = ControllerConf.getControllerConf();

        } catch (Exception e) {
            e.printStackTrace();
        }

        RocketmqBrokerTopicObserver rocketmqBrokerTopicObserver =
                new RocketmqBrokerTopicObserver("demo", "10.241.11.211:9876");

        HelixRocketmqReplicatorManager helixRocketmqReplicatorManager = new HelixRocketmqReplicatorManager(controllerConf, rocketmqBrokerTopicObserver);

        helixRocketmqReplicatorManager.start();

        System.out.println("------------------ START ----------------------");
        System.out.println("------------------ 手工开启一个实例 ----------------------");

        //此时增加一个实例
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //新增一个topic queue
        helixRocketmqReplicatorManager.addTopicToMirrorMaker("rr_demo4", rocketmqBrokerTopicObserver.getTopicPartition("rr_demo4"));

        try {
            Thread.sleep(500000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        helixRocketmqReplicatorManager.stop();
    }

    @Test
    public void autoTopicWhitelistingManagerTest(){

        ControllerConf controllerConf = null;
        try {
            controllerConf = ControllerConf.getControllerConf();

        } catch (Exception e) {
            e.printStackTrace();
        }

        HelixRocketMQReplicatorMetricsReporter.init(controllerConf);

        RocketmqBrokerTopicObserver srcRocketmqBrokerTopicObserver =
                new RocketmqBrokerTopicObserver("demo", "10.241.11.211:9876");

        HelixRocketmqReplicatorManager helixRocketmqReplicatorManager = new HelixRocketmqReplicatorManager(controllerConf, srcRocketmqBrokerTopicObserver);

        helixRocketmqReplicatorManager.start();

        RocketmqBrokerTopicObserver desRocketmqBrokerTopicObserver =
                new RocketmqBrokerTopicObserver("demo1", "10.241.16.133:9876");

        AutoTopicWhitelistingManager autoTopicWhitelistingManager = new AutoTopicWhitelistingManager(
                srcRocketmqBrokerTopicObserver,
                desRocketmqBrokerTopicObserver,
                helixRocketmqReplicatorManager,
                20,
                3,
                controllerConf
        );

        autoTopicWhitelistingManager.getCandidateTopicsToWhitelist();
    }

}
