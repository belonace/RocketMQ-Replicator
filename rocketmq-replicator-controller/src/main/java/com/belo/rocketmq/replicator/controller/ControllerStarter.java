package com.belo.rocketmq.replicator.controller;

import com.belo.rocketmq.replicator.controller.core.AutoTopicWhitelistingManager;
import com.belo.rocketmq.replicator.controller.core.HelixRocketmqReplicatorManager;
import com.belo.rocketmq.replicator.controller.core.RocketmqBrokerTopicObserver;
import com.belo.rocketmq.replicator.controller.reporter.HelixRocketMQReplicatorMetricsReporter;
import com.belo.rocketmq.replicator.controller.validation.ValidationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * controller 启动类
 * Created by liqiang on 2017/9/27.
 */
public class ControllerStarter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerStarter.class);

    private static final String DEST_ROCKETMQ_CLUSTER = "destRocketMQCluster";
    private static final String SRC_ROCKETMQ_CLUSTER = "srcRocketMQCluster";
    private final ControllerConf config;

    private final HelixRocketmqReplicatorManager helixRocketmqReplicatorManager;
    private final ValidationManager validationManager;
    //private final SourceRocketMQClusterValidationManager sourceRocketMQClusterValidationManager;
    private final AutoTopicWhitelistingManager autoTopicWhitelistingManager;

    //暂时不考虑文件的备份，因为没有什么用
    //private final ClusterInfoBackupManager clusterInfoBackupManager;

    private final Map<String, RocketmqBrokerTopicObserver> rocketmqBrokerTopicObserverMap = new HashMap<String, RocketmqBrokerTopicObserver>();

    public ControllerStarter(ControllerConf conf) {
        LOGGER.info("Trying to init ControllerStarter with config: {}", conf);

        this.config = conf;
        HelixRocketMQReplicatorMetricsReporter.init(this.config);

        rocketmqBrokerTopicObserverMap.put(SRC_ROCKETMQ_CLUSTER,
                new RocketmqBrokerTopicObserver(SRC_ROCKETMQ_CLUSTER, config.getSrcRocketMQPath()));

        rocketmqBrokerTopicObserverMap.put(DEST_ROCKETMQ_CLUSTER,
                new RocketmqBrokerTopicObserver(DEST_ROCKETMQ_CLUSTER, config.getDestRocketMQPath()));

        helixRocketmqReplicatorManager = new HelixRocketmqReplicatorManager(config, rocketmqBrokerTopicObserverMap.get(SRC_ROCKETMQ_CLUSTER));

        validationManager = new ValidationManager(helixRocketmqReplicatorManager);

        //sourceRocketMQClusterValidationManager = new SourceRocketMQClusterValidationManager(
        //        rocketmqBrokerTopicObserverMap.get(SRC_ROCKETMQ_CLUSTER),
        //        helixRocketmqReplicatorManager, true);

        autoTopicWhitelistingManager = new AutoTopicWhitelistingManager(rocketmqBrokerTopicObserverMap.get(SRC_ROCKETMQ_CLUSTER),
                rocketmqBrokerTopicObserverMap.get(DEST_ROCKETMQ_CLUSTER), helixRocketmqReplicatorManager,
                config.getRefreshTimeInSeconds(), config.getInitWaitTimeInSeconds(), this.config);

        //clusterInfoBackupManager = new ClusterInfoBackupManager(helixRocketmqReplicatorManager,
        //        new FileBackUpHandler(this.config.getLocalBackupFilePath()), config);
    }

    public HelixRocketmqReplicatorManager getHelixResourceManager() {
        return helixRocketmqReplicatorManager;
    }

    public void start() throws Exception {

        LOGGER.info("injecting conf and resource manager to the api context");

        try {
            LOGGER.info("starting helix mirror maker manager");

            helixRocketmqReplicatorManager.start();
            validationManager.start();
            //sourceRocketMQClusterValidationManager.start();
            //clusterInfoBackupManager.start();

            autoTopicWhitelistingManager.start();

        } catch (final Exception e) {
            LOGGER.error("Caught exception while starting controller", e);
            throw e;
        }
    }

    public void stop() {
        try {
            LOGGER.info("stopping broker topic observers");
            for (String key : rocketmqBrokerTopicObserverMap.keySet()) {
                try {
                    RocketmqBrokerTopicObserver observer = rocketmqBrokerTopicObserverMap.get(key);
                    observer.stop();
                } catch (Exception e) {
                    LOGGER.error("Failed to stop rocketMQBrokerTopicObserver: {}!", key);
                }
            }

            LOGGER.info("stopping resource manager");

            helixRocketmqReplicatorManager.stop();

        } catch (final Exception e) {
            LOGGER.error("Caught exception", e);
        }
    }

    public static ControllerStarter init() {
        ControllerConf conf = null;

        try {
            conf = ControllerConf.getControllerConf();
        } catch (Exception e) {
            LOGGER.error("Get controller configurations error!", e);
            System.exit(0);
        }

        if (!conf.checkWorkerConf()) {
            LOGGER.info("Not valid controller configurations!");
            System.exit(0);
        }

        final ControllerStarter starter = new ControllerStarter(conf);
        return starter;
    }

    public static void main(String[] args) {

        final ControllerStarter controllerStarter = ControllerStarter.init();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    controllerStarter.stop();
                } catch (Exception e) {
                    LOGGER.error("Caught error during shutdown! ", e);
                }
            }
        });

        try {
            controllerStarter.start();
        } catch (Exception e) {
            LOGGER.error("Cannot start Helix Mirror Maker Controller: ", e);
        }

    }
}
