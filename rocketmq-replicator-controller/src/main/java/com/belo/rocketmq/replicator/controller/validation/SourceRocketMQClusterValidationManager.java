package com.belo.rocketmq.replicator.controller.validation;

import com.belo.rocketmq.replicator.controller.core.HelixRocketmqReplicatorManager;
import com.belo.rocketmq.replicator.controller.core.RocketmqBrokerTopicObserver;
import com.belo.rocketmq.replicator.controller.reporter.HelixRocketMQReplicatorMetricsReporter;
import com.codahale.metrics.Counter;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 源RocketMQ集群验证，查询每个topic的queue在源处有没有扩展，如有扩展则修改Controller的状态，使之平衡
 * Created by liqiang on 2017/9/28.
 */
public class SourceRocketMQClusterValidationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceRocketMQClusterValidationManager.class);

    private final HelixRocketmqReplicatorManager helixRocketmqReplicatorManager;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private int timeValue = 60;
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    // Metrics
    private final static String MISMATCHED_METRICS_FORMAT = "rocketmqAndIdealstatesDiscrepancy.mismatched.topic.%s";
    private final Counter numMissingTopics = new Counter();
    private final Counter numMismatchedTopics = new Counter();
    private final Counter numMismatchedTopicPartitions = new Counter();
    private final RocketmqBrokerTopicObserver sourceRocketMQTopicObserver;
    private final boolean enableAutoTopicExpansion;
    private final Counter numAutoExpandedTopics = new Counter();
    private final Counter numAutoExpandedTopicPartitions = new Counter();

    private final Map<String, Counter> mismatchedTopicPartitionsCounter = new HashMap<String, Counter>();

    public SourceRocketMQClusterValidationManager(RocketmqBrokerTopicObserver sourceRocketMQTopicObserver,
                                                  HelixRocketmqReplicatorManager helixRocketmqReplicatorManager, boolean enableAutoTopicExpansion) {
        this.sourceRocketMQTopicObserver = sourceRocketMQTopicObserver;
        this.helixRocketmqReplicatorManager = helixRocketmqReplicatorManager;
        this.enableAutoTopicExpansion = enableAutoTopicExpansion;
    }

    public void start() {
        registerMetrics();

        // Report current status every one minutes.
        LOGGER.info("Trying to schedule a source rocketmq cluster validation job at rate {} {} !", this.timeValue, this.timeUnit.toString());

        this.executorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (helixRocketmqReplicatorManager.isLeader()) {
                    LOGGER.info("Trying to run the source rocketmq cluster info validation job");
                    validateSourceRocketMQCluster();
                } else {
                    cleanupMetrics();
                    LOGGER.debug("Not leader, skip validation for source rocketmq cluster!");
                }
            }

            private void cleanupMetrics() {
                numMissingTopics.dec(numMissingTopics.getCount());
                numMismatchedTopics.dec(numMismatchedTopics.getCount());
                numMismatchedTopicPartitions.dec(numMismatchedTopicPartitions.getCount());

                for (String topic : mismatchedTopicPartitionsCounter.keySet()) {
                    Counter counter = mismatchedTopicPartitionsCounter.get(topic);
                    counter.dec(counter.getCount());
                }
            }
        }, 30, timeValue, timeUnit);
    }

    public void validateSourceRocketMQCluster() {
        Set<String> notExistedTopics = new HashSet<String>();
        Map<String, Integer> misMatchedPartitionNumberTopics = new HashMap<String, Integer>();
        int numMismatchedTopicPartitions = 0;
        for (String topic : this.helixRocketmqReplicatorManager.getTopicLists()) {
            TopicPublishInfo topicPublishInfo = this.sourceRocketMQTopicObserver.getTopicPartition(topic);
            if (topicPublishInfo == null) {
                LOGGER.warn("Topic {} is not in source rocketmq broker!", topic);
                notExistedTopics.add(topic);
            } else {
                int numPartitionsInMirrorMaker = this.helixRocketmqReplicatorManager.getIdealStateForTopic(topic).getNumPartitions();
                if (numPartitionsInMirrorMaker != topicPublishInfo.getMessageQueueList().size()) {
                    int mismatchedPartitions = Math.abs(numPartitionsInMirrorMaker - topicPublishInfo.getMessageQueueList().size());
                    if (this.enableAutoTopicExpansion && (topicPublishInfo.getMessageQueueList().size() > numPartitionsInMirrorMaker)) {
                        // Only do topic expansion
                        LOGGER.warn("Trying to expand topic {} from {} partitions in mirror maker to {} from source rocketmq broker!",
                                topic, numPartitionsInMirrorMaker, topicPublishInfo.getMessageQueueList().size());

                        this.numAutoExpandedTopics.inc();
                        this.numAutoExpandedTopicPartitions.inc(mismatchedPartitions);
                        this.helixRocketmqReplicatorManager.expandTopicInMirrorMaker(topic, topicPublishInfo);
                    } else {
                        numMismatchedTopicPartitions += mismatchedPartitions;
                        misMatchedPartitionNumberTopics.put(topic, mismatchedPartitions);
                        LOGGER.warn("Number of partitions not matched for topic {} between mirrormaker:{} and source rocketmq broker: {}!",
                                topic, numPartitionsInMirrorMaker, topicPublishInfo.getMessageQueueList().size());
                    }
                }
            }
        }
        
        if (this.helixRocketmqReplicatorManager.isLeader()) {
            updateMetrics(notExistedTopics.size(), misMatchedPartitionNumberTopics.size(), numMismatchedTopicPartitions, misMatchedPartitionNumberTopics);
        }

    }

    private void registerMetrics() {
        try {
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    "rocketmqAndIdealstatesDiscrepancy.missing.topics",
                    this.numMissingTopics);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    "rocketmqAndIdealstatesDiscrepancy.mismatched.topics",
                    this.numMismatchedTopics);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    "rocketmqAndIdealstatesDiscrepancy.mismatched.topicPartitions",
                    this.numMismatchedTopicPartitions);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    "rocketmqAndIdealstatesDiscrepancy.autoExpansion.topics",
                    this.numAutoExpandedTopics);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    "rocketmqAndIdealstatesDiscrepancy.autoExpansion.topicPartitions",
                    this.numAutoExpandedTopicPartitions);
        } catch (Exception e) {
            LOGGER.error("Failed to register metrics to HelixRocketmqMirrorMakerMetricsReporter " + e);
        }
    }

    private synchronized void updateMetrics(int numMissingTopics, int numMismatchedTopics,
                                            int numMismatchedTopicPartitions, Map<String, Integer> misMatchedPartitionNumberTopics) {
        this.numMissingTopics.inc(numMissingTopics - this.numMissingTopics.getCount());
        this.numMismatchedTopics.inc(numMismatchedTopics - this.numMismatchedTopics.getCount());
        this.numMismatchedTopicPartitions
                .inc(numMismatchedTopicPartitions - this.numMismatchedTopicPartitions.getCount());

        for (String topic : misMatchedPartitionNumberTopics.keySet()) {
            if (!this.mismatchedTopicPartitionsCounter.containsKey(topic)) {
                Counter topicPartitionCounter = new Counter();
                try {
                    HelixRocketMQReplicatorMetricsReporter.get().getRegistry().register(
                            getMismatchedTopicMetricName(topic), topicPartitionCounter);
                } catch (Exception e) {
                    LOGGER.error("Error registering metrics!", e);
                }
                this.mismatchedTopicPartitionsCounter.put(topic, topicPartitionCounter);
            }
        }
        for (String topic : this.mismatchedTopicPartitionsCounter.keySet()) {
            Counter counter = this.mismatchedTopicPartitionsCounter.get(topic);
            if (!misMatchedPartitionNumberTopics.containsKey(topic)) {
                counter.dec(counter.getCount());
            } else {
                counter.inc(misMatchedPartitionNumberTopics.get(topic) - counter.getCount());
            }
        }
    }

    private static String getMismatchedTopicMetricName(String topicName) {
        return String.format(MISMATCHED_METRICS_FORMAT, topicName.replace(".", "_"));
    }

}
