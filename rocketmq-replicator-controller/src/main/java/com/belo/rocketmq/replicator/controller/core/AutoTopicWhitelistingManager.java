package com.belo.rocketmq.replicator.controller.core;

import com.belo.rocketmq.replicator.controller.ControllerConf;
import com.belo.rocketmq.replicator.controller.reporter.HelixRocketMQReplicatorMetricsReporter;
import com.codahale.metrics.Counter;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by liqiang on 2017/9/28.
 */
public class AutoTopicWhitelistingManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoTopicWhitelistingManager.class);

    private final HelixRocketmqReplicatorManager helixRocketmqReplicatorManager;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    private final int refreshTimeInSec;
    private final int initWaitTimeInSec;
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    // Metrics
    private final static String AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE = "AutoTopicWhitelistManager.%s";

    private final Counter numWhitelistedTopics = new Counter();
    private final Counter numAutoExpandedTopics = new Counter();
    private final Counter numErrorTopics = new Counter();

    private final Counter numOfAutoWhitelistingRuns = new Counter();
    private final RocketmqBrokerTopicObserver srcRocketMQTopicObserver;
    private final RocketmqBrokerTopicObserver destRocketMQTopicObserver;

    private ControllerConf controllerConf;

    public AutoTopicWhitelistingManager(RocketmqBrokerTopicObserver srcRocketMQTopicObserver,
                                        RocketmqBrokerTopicObserver destRocketMQTopicObserver,
                                        HelixRocketmqReplicatorManager helixRocketmqReplicatorManager,
                                        int refreshTimeInSec,
                                        int initWaitTimeInSec,
                                        ControllerConf controllerConf) {

        this.srcRocketMQTopicObserver = srcRocketMQTopicObserver;
        this.destRocketMQTopicObserver = destRocketMQTopicObserver;
        this.helixRocketmqReplicatorManager = helixRocketmqReplicatorManager;
        this.refreshTimeInSec = refreshTimeInSec;
        this.initWaitTimeInSec = initWaitTimeInSec;
        this.controllerConf = controllerConf;
    }

    public void start() {
        registerMetrics();

        LOGGER.info("Trying to schedule auto topic whitelisting job at rate {} {} !", this.refreshTimeInSec, this.timeUnit.toString());

        this.executorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (helixRocketmqReplicatorManager.isLeader()) {
                    numOfAutoWhitelistingRuns.inc();
                    LOGGER.info("Trying to run topic whitelisting job");
                    try {
                        getCandidateTopicsToWhitelist();
                    } catch (Exception e) {
                        LOGGER.error("Failed to get candidate topics: ", e);
                    }

                    numErrorTopics.dec(numErrorTopics.getCount());
                } else {
                    LOGGER.debug("Not leader, skip auto topic whitelisting!");
                }

            }
        }, Math.min(this.initWaitTimeInSec, this.refreshTimeInSec), this.refreshTimeInSec, this.timeUnit);
    }

    public void getCandidateTopicsToWhitelist() {

        LOGGER.info("WriteListTopics={}", this.controllerConf.getWriteList());

        Set<String> writeTopic = new HashSet<String>();

        if (StringUtils.isBlank(this.controllerConf.getWriteList())) {
            return;
        }

        String[] topics = this.controllerConf.getWriteList().split(",");

        for (String topic : topics) {
            writeTopic.add(topic.trim());
        }

        Set<String> srcRocketMQTopics = new HashSet<String>(this.srcRocketMQTopicObserver.getAllTopics());
        Set<String> destRocketMQTopics = new HashSet<String>(this.destRocketMQTopicObserver.getAllTopics());
        Set<String> controllerTopics = new HashSet<String>(this.helixRocketmqReplicatorManager.getTopicLists());

        for (String topic : writeTopic) {
            if (srcRocketMQTopics.contains(topic) && destRocketMQTopics.contains(topic)) {
                if (controllerTopics.contains(topic)) {
                    int numPartitionsInHelix = this.helixRocketmqReplicatorManager.getIdealStateForTopic(topic).getNumPartitions();
                    int numPartitionsInSrcBroker = this.srcRocketMQTopicObserver.getTopicPartition(topic).getMessageQueueList().size();
                    if (numPartitionsInHelix != numPartitionsInSrcBroker) {
                        TopicPublishInfo topicPublishInfo = this.srcRocketMQTopicObserver.getTopicPartition(topic);
                        if (topicPublishInfo == null) {
                            LOGGER.error("Shouldn't hit here, don't know why topic {} is not in src RocketMQ cluster", topic);
                            this.numErrorTopics.inc();
                        } else {
                            LOGGER.info("Trying to expand topic: {} with {} partitions", topic, topicPublishInfo.getMessageQueueList().size());
                            this.helixRocketmqReplicatorManager.expandTopicInMirrorMaker(topic, topicPublishInfo);
                            this.numAutoExpandedTopics.inc();

                        }
                    }
                } else {
                    TopicPublishInfo topicPublishInfo = this.srcRocketMQTopicObserver.getTopicPartition(topic);
                    if (topicPublishInfo == null) {
                        LOGGER.error("Shouldn't hit here, don't know why topic {} is not in src RocketMQ cluster", topic);
                        this.numErrorTopics.inc();
                    } else {
                        LOGGER.info("Trying to whitelist topic: {} with {} partitions", topic, topicPublishInfo.getMessageQueueList().size());
                        this.helixRocketmqReplicatorManager.addTopicToMirrorMaker(topic, topicPublishInfo);
                        this.numWhitelistedTopics.inc();
                    }
                }
            }
        }

        for(String topic : controllerTopics){
            if(!writeTopic.contains(topic)){
                LOGGER.info("Delete Topic :{}", topic);
                this.helixRocketmqReplicatorManager.deleteTopicInMirrorMaker(topic);
            }
        }

    }

    private void registerMetrics() {
        try {
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numOfAutoWhitelistingRuns"),
                    this.numOfAutoWhitelistingRuns);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numWhitelistedTopics"),
                    this.numWhitelistedTopics);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numAutoExpandedTopics"),
                    this.numAutoExpandedTopics);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    String.format(AUTO_TOPIC_WHITELIST_MANAGER_METRICS_TEMPLATE, "numErrorTopics"),
                    this.numErrorTopics);
        } catch (Exception e) {
            LOGGER.error("Got exception during register metrics");
        }
    }

}
