package com.belo.rocketmq.replicator.controller.validation;

import com.alibaba.fastjson.JSONObject;
import com.belo.rocketmq.replicator.controller.core.HelixRocketmqReplicatorManager;
import com.belo.rocketmq.replicator.controller.reporter.HelixRocketMQReplicatorMetricsReporter;
import com.codahale.metrics.Counter;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by liqiang on 2017/9/28.
 */
public class ValidationManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationManager.class);
    private static final String IDEALSTATE_PER_WORKER_METRICS_FORMAT = "idealStates.topicPartitions.%s.totalNumber";
    private static final String EXTERNALVIEW_PER_WORKER_METRICS_FORMAT = "externalView.topicPartitions.%s.totalNumber";

    private final HelixRocketmqReplicatorManager helixRocketmqReplicatorManager;
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private int timeValue = 60;
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    // Metrics

    private final Counter isLeaderCounter = new Counter();
    private final Counter numServingTopics = new Counter();
    private final Counter numTopicPartitions = new Counter();
    private final Counter numOnlineTopicPartitions = new Counter();
    private final Counter numOfflineTopicPartitions = new Counter();
    private final Counter numErrorTopicPartitions = new Counter();
    private final Counter numErrorTopics = new Counter();

    private final Map<String, Counter> idealStatePerWorkerTopicPartitionCounter = new HashMap<String, Counter>();
    private final Map<String, Counter> externalViewPerWorkerTopicPartitionCounter = new HashMap<String, Counter>();

    public ValidationManager(HelixRocketmqReplicatorManager helixRocketmqReplicatorManager) {
        this.helixRocketmqReplicatorManager = helixRocketmqReplicatorManager;
    }

    public void start() {
        registerMetrics();

        // Report current status every one minutes.
        LOGGER.info("Trying to schedule a validation job at rate {} {} !", this.timeValue, this.timeUnit.toString());

        this.executorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                if (helixRocketmqReplicatorManager.isLeader()) {
                    isLeaderCounter.inc(1 - isLeaderCounter.getCount());
                    LOGGER.info("Trying to run the validation job");
                    validateExternalView();
                } else {
                    cleanupMetrics();

                    LOGGER.debug("Not leader, skip validation!");
                }
            }

            private void cleanupMetrics() {
                isLeaderCounter.dec(isLeaderCounter.getCount());
                numServingTopics.dec(numServingTopics.getCount());
                numTopicPartitions.dec(numTopicPartitions.getCount());
                numOnlineTopicPartitions.dec(numOnlineTopicPartitions.getCount());
                numOfflineTopicPartitions.dec(numOfflineTopicPartitions.getCount());
                numErrorTopicPartitions.dec(numErrorTopicPartitions.getCount());
                numErrorTopics.dec(numErrorTopics.getCount());
                for (Counter counter : idealStatePerWorkerTopicPartitionCounter.values()) {
                    counter.dec(counter.getCount());
                }
                for (Counter counter : externalViewPerWorkerTopicPartitionCounter.values()) {
                    counter.dec(counter.getCount());
                }

            }
        }, 120, timeValue, this.timeUnit);
    }

    private void registerMetrics() {
        try {
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("leader.counter",
                    isLeaderCounter);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("topic.totalNumber",
                    numServingTopics);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("topic.errorNumber",
                    numErrorTopics);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("topic.partitions.totalNumber",
                    numTopicPartitions);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("topic.partitions.onlineNumber",
                    numOnlineTopicPartitions);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("topic.partitions.offlineNumber",
                    numOfflineTopicPartitions);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("topic.partitions.errorNumber",
                    numErrorTopicPartitions);
        } catch (Exception e) {
            LOGGER.error("Error registering metrics!", e);
        }
    }

    public String validateExternalView() {
        try {
            Map<String, Integer> topicPartitionMapForIdealState = new HashMap<String, Integer>();
            Map<String, Integer> topicPartitionMapForExternalView = new HashMap<String, Integer>();
            int numOnlineTopicPartitions = 0;
            int numOfflineTopicPartitions = 0;
            int numErrorTopicPartitions = 0;
            int numTopicPartitions = 0;
            int numServingTopics = 0;
            int numErrorTopics = 0;
            for (String topicName : this.helixRocketmqReplicatorManager.getTopicLists()) {
                numServingTopics++;
                IdealState idealStateForTopic = this.helixRocketmqReplicatorManager.getIdealStateForTopic(topicName);
                ExternalView externalViewForTopic = this.helixRocketmqReplicatorManager.getExternalViewForTopic(topicName);

                numTopicPartitions += idealStateForTopic.getNumPartitions();
                if (idealStateForTopic.getNumPartitions() != externalViewForTopic.getPartitionSet().size()) {
                    numErrorTopics++;
                    LOGGER.error("For topic:{}, number of partitions for IdealState: {} doesn't match ExternalView: {}",
                            topicName, idealStateForTopic, externalViewForTopic);
                }

                // IdealState Counting
                updateIdealstateInfo(topicPartitionMapForIdealState, idealStateForTopic);
                // ExternalView Counting
                for (String partition : externalViewForTopic.getPartitionSet()) {
                    Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
                    for (String instance : stateMap.keySet()) {
                        String state = stateMap.get(instance);
                        if (!topicPartitionMapForExternalView.containsKey(instance)) {
                            topicPartitionMapForExternalView.put(instance, 1);
                        } else {
                            topicPartitionMapForExternalView.put(instance, topicPartitionMapForExternalView.get(instance) + 1);
                        }
                        if ("ONLINE".equalsIgnoreCase(state)) {
                            numOnlineTopicPartitions++;
                        } else if ("OFFLINE".equalsIgnoreCase(state)) {
                            numOfflineTopicPartitions++;
                        } else if ("ERROR".equalsIgnoreCase(state)) {
                            numErrorTopicPartitions++;
                        }
                    }
                }
            }

            if (this.helixRocketmqReplicatorManager.isLeader()) {
                updateMetrics(numOnlineTopicPartitions, numOfflineTopicPartitions, numErrorTopicPartitions, numTopicPartitions, numServingTopics, numErrorTopics);
                updatePerWorkerISMetrics(topicPartitionMapForExternalView);
                updatePerWorkerEVMetrics(topicPartitionMapForExternalView);
            }
            JSONObject perWorkerISCounterJson = constructPerWorkerISCounterJson(topicPartitionMapForIdealState);
            JSONObject perWorkerEVCounterJson = constructPerWorkerEVCounterJson(topicPartitionMapForExternalView);
            JSONObject validationResultJson = constructValidationResultJson(numOnlineTopicPartitions,
                    numOfflineTopicPartitions, numErrorTopicPartitions, numTopicPartitions,
                    numServingTopics, numErrorTopics, perWorkerISCounterJson, perWorkerEVCounterJson);
            return validationResultJson.toJSONString();
        } catch (Exception e) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("excpetion", e);
            return jsonObject.toJSONString();
        }
    }

    private JSONObject constructValidationResultJson(int numOnlineTopicPartitions,
                                                     int numOfflineTopicPartitions,
                                                     int numErrorTopicPartitions, int numTopicPartitions, int numServingTopics, int numErrorTopics,
                                                     JSONObject perWorkerISCounterJson, JSONObject perWorkerEVCounterJson) {
        JSONObject validationResultJson = new JSONObject();
        validationResultJson.put("numTopics", numServingTopics);
        validationResultJson.put("numTopicPartitions", numTopicPartitions);
        validationResultJson.put("numOnlineTopicPartitions", numOnlineTopicPartitions);
        validationResultJson.put("numOfflineTopicPartitions", numOfflineTopicPartitions);
        validationResultJson.put("numErrorTopicPartitions", numErrorTopicPartitions);
        validationResultJson.put("numErrorTopics", numErrorTopics);
        validationResultJson.put("IdealState", perWorkerISCounterJson);
        validationResultJson.put("ExternalView", perWorkerEVCounterJson);
        return validationResultJson;
    }

    private JSONObject constructPerWorkerISCounterJson(
            Map<String, Integer> topicPartitionMapForIdealState) {
        JSONObject perWorkISCounterJson = new JSONObject();
        for (String worker : topicPartitionMapForIdealState.keySet()) {
            perWorkISCounterJson.put(worker, topicPartitionMapForIdealState.get(worker));
        }
        return perWorkISCounterJson;
    }

    private JSONObject constructPerWorkerEVCounterJson(
            Map<String, Integer> topicPartitionMapForExternalView) {
        JSONObject perWorkEVCounterJson = new JSONObject();
        for (String worker : topicPartitionMapForExternalView.keySet()) {
            perWorkEVCounterJson.put(worker, topicPartitionMapForExternalView.get(worker));
        }
        return perWorkEVCounterJson;
    }

    private void updatePerWorkerISMetrics(Map<String, Integer> topicPartitionMapForIdealState) {
        for (String worker : topicPartitionMapForIdealState.keySet()) {
            if (!this.idealStatePerWorkerTopicPartitionCounter.containsKey(worker)) {
                Counter workCounter = new Counter();
                try {
                    HelixRocketMQReplicatorMetricsReporter.get().getRegistry().register(
                            getIdealStatePerWorkMetricName(worker), workCounter);
                } catch (Exception e) {
                    LOGGER.error("Error registering metrics!", e);
                }
                this.idealStatePerWorkerTopicPartitionCounter.put(worker, workCounter);
            }
            Counter counter = this.idealStatePerWorkerTopicPartitionCounter.get(worker);
            counter.inc(topicPartitionMapForIdealState.get(worker) -
                    counter.getCount());
        }
        for (String worker : this.idealStatePerWorkerTopicPartitionCounter.keySet()) {
            if (!topicPartitionMapForIdealState.containsKey(worker)) {
                Counter counter = this.idealStatePerWorkerTopicPartitionCounter.get(worker);
                counter.dec(counter.getCount());
            }
        }
    }

    private void updatePerWorkerEVMetrics(Map<String, Integer> topicPartitionMapForExternalView) {

        for (String worker : topicPartitionMapForExternalView.keySet()) {
            if (!this.externalViewPerWorkerTopicPartitionCounter.containsKey(worker)) {
                Counter workCounter = new Counter();
                try {
                    HelixRocketMQReplicatorMetricsReporter.get().getRegistry().register(
                            getExternalViewPerWorkMetricName(worker), workCounter);
                } catch (Exception e) {
                    LOGGER.error("Error registering metrics!", e);
                }
                this.externalViewPerWorkerTopicPartitionCounter.put(worker, workCounter);
            }
            Counter counter = this.externalViewPerWorkerTopicPartitionCounter.get(worker);
            counter.inc(topicPartitionMapForExternalView.get(worker) -
                    counter.getCount());
        }

        for (String worker : this.externalViewPerWorkerTopicPartitionCounter.keySet()) {
            if (!topicPartitionMapForExternalView.containsKey(worker)) {
                Counter counter = this.externalViewPerWorkerTopicPartitionCounter.get(worker);
                counter.dec(counter.getCount());
            }
        }
    }

    private void updateMetrics(int numOnlineTopicPartitions,
                                            int numOfflineTopicPartitions,
                                            int numErrorTopicPartitions, int numTopicPartitions, int numServingTopics,
                                            int numErrorTopics) {
        this.numServingTopics
                .inc(numServingTopics - this.numServingTopics.getCount());
        this.numTopicPartitions
                .inc(numTopicPartitions - this.numTopicPartitions.getCount());
        this.numOnlineTopicPartitions
                .inc(numOnlineTopicPartitions - this.numOnlineTopicPartitions.getCount());
        this.numOfflineTopicPartitions
                .inc(numOfflineTopicPartitions - this.numOfflineTopicPartitions.getCount());
        this.numErrorTopicPartitions
                .inc(numErrorTopicPartitions - this.numErrorTopicPartitions.getCount());
        this.numErrorTopics
                .inc(numErrorTopics - this.numErrorTopics.getCount());
    }

    private void updateIdealstateInfo(Map<String, Integer> topicPartitionMapForIdealState, IdealState idealStateForTopic) {
        for (String partition : idealStateForTopic.getPartitionSet()) {
            Map<String, String> idealStatesMap = idealStateForTopic.getInstanceStateMap(partition);
            for (String instance : idealStatesMap.keySet()) {
                if (!topicPartitionMapForIdealState.containsKey(instance)) {
                    topicPartitionMapForIdealState.put(instance, 1);
                } else {
                    topicPartitionMapForIdealState.put(instance,
                            topicPartitionMapForIdealState.get(instance) + 1);
                }
            }
        }
    }

    private static String getIdealStatePerWorkMetricName(String worker) {
        return String.format(IDEALSTATE_PER_WORKER_METRICS_FORMAT, worker);
    }

    private static String getExternalViewPerWorkMetricName(String worker) {
        return String.format(EXTERNALVIEW_PER_WORKER_METRICS_FORMAT, worker);
    }
}
