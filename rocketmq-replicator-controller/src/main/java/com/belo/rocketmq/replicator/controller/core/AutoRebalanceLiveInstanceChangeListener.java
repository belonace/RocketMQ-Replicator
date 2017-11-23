package com.belo.rocketmq.replicator.controller.core;

import com.belo.rocketmq.replicator.controller.reporter.HelixRocketMQReplicatorMetricsReporter;
import com.belo.rocketmq.replicator.controller.utils.HelixUtils;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 重新均衡监听器,监听实例的变化
 * Created by liqiang on 2017/9/27.
 */
public class AutoRebalanceLiveInstanceChangeListener implements LiveInstanceChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AutoRebalanceLiveInstanceChangeListener.class);

    private final ScheduledExecutorService delayedScheuler = Executors.newSingleThreadScheduledExecutor();

    private final HelixRocketmqReplicatorManager helixRocketmqReplicatorManager;
    private final HelixManager helixManager;

    private final Counter numLiveInstances = new Counter();
    private final Meter rebalanceRate = new Meter();
    private final Timer rebalanceTimer = new Timer();
    private RocketmqBrokerTopicObserver srcBrokerTopicObserver;

    //指定controller的Rebalance时间
    private final int delayedAutoReblanceTimeInSeconds;

    public AutoRebalanceLiveInstanceChangeListener(HelixRocketmqReplicatorManager helixRocketmqReplicatorManager,
                                                   HelixManager helixManager, int delayedAutoReblanceTimeInSeconds,
                                                   RocketmqBrokerTopicObserver srcBrokerTopicObserver) {
        this.helixRocketmqReplicatorManager = helixRocketmqReplicatorManager;
        this.helixManager = helixManager;
        this.delayedAutoReblanceTimeInSeconds = delayedAutoReblanceTimeInSeconds;
        this.srcBrokerTopicObserver = srcBrokerTopicObserver;
        LOGGER.info("Delayed Auto Reblance Time In Seconds: {}", this.delayedAutoReblanceTimeInSeconds);
        registerMetrics();
    }

    private void registerMetrics() {
        try {
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("worker.liveInstances", this.numLiveInstances);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("worker.rebalance.rate", this.rebalanceRate);
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric("worker.rebalance.timer", this.rebalanceTimer);
        } catch (Exception e) {
            LOGGER.error("Error registering metrics!", e);
        }
    }

    //通知接口
    public void onLiveInstanceChange(final List<LiveInstance> liveInstances, NotificationContext changeContext) {

        LOGGER.info("AutoRebalanceLiveInstanceChangeListener.onLiveInstanceChange() wakes up!");

        this.delayedScheuler.schedule(new Runnable() {
            public void run() {
                try {
                    rebalanceCurrentCluster(helixRocketmqReplicatorManager.getCurrentLiveInstances());
                } catch (Exception e) {
                    LOGGER.error("Got exception during rebalance the whole cluster! ", e);
                }
            }
        }, this.delayedAutoReblanceTimeInSeconds, TimeUnit.SECONDS);
    }

    public synchronized void rebalanceCurrentCluster(List<LiveInstance> liveInstances) {
        Timer.Context context = this.rebalanceTimer.time();
        LOGGER.info("AutoRebalanceLiveInstanceChangeListener.rebalanceCurrentCluster() wakes up!");
        try {
            this.numLiveInstances.inc(liveInstances.size() - this.numLiveInstances.getCount());

            if (!this.helixRocketmqReplicatorManager.isLeader()) {
                LOGGER.info("Not leader, do nothing!");
                return;
            }
            if (!this.helixRocketmqReplicatorManager.isAutoBalancingEnabled()) {
                LOGGER.info("Is leader, but auto-balancing is disabled, do nothing!");
                return;
            }
            if (liveInstances.isEmpty()) {
                LOGGER.info("No live instances, do nothing!");
                return;
            }

            final Map<String /* instanceName */, Set<MessageQueue>> instanceToTopicPartitionMap = HelixUtils.getInstanceToTopicQueuesMap(this.helixManager);

            Set<MessageQueue> unassignedTopicPartitions = HelixUtils.getUnassignedQueues(this.helixManager, this.srcBrokerTopicObserver);

            if (instanceToTopicPartitionMap.isEmpty() && unassignedTopicPartitions.isEmpty()) {
                LOGGER.info("No topic got assigned yet, do nothing!");
                return;
            }

            Set<InstanceTopicQueueHolder> newAssignment = rescaleInstanceToTopicPartitionMap(liveInstances, instanceToTopicPartitionMap,
                            unassignedTopicPartitions);

            if (newAssignment == null) {
                LOGGER.info("No Live Instances got changed, do nothing!");
                return;
            }

            LOGGER.info("Trying to fetch IdealStatesMap from current assignment!");

            Map<String, IdealState> idealStatesFromAssignment = HelixUtils.getIdealStatesFromAssignment(newAssignment);

            LOGGER.info("Trying to assign new IdealStatesMap!");

            assignIdealStates(this.helixManager, idealStatesFromAssignment);

            this.helixRocketmqReplicatorManager.updateCurrentServingInstance();

            this.rebalanceRate.mark();

        } finally {
            context.close();
        }
    }

    private static Set<InstanceTopicQueueHolder> rescaleInstanceToTopicPartitionMap(
            List<LiveInstance> liveInstances,
            Map<String, Set<MessageQueue>> instanceToTopicPartitionMap,
            Set<MessageQueue> unassignedTopicPartitions) {

        Set<String> newInstances = getAddedInstanceSet(getLiveInstanceName(liveInstances), instanceToTopicPartitionMap.keySet());
        Set<String> removedInstances = getRemovedInstanceSet(getLiveInstanceName(liveInstances), instanceToTopicPartitionMap.keySet());

        if (newInstances.isEmpty() && removedInstances.isEmpty()) {
            return null;
        }

        LOGGER.info("Trying to rescale cluster with new instances - " + Arrays.toString(
                newInstances.toArray(new String[0])) + " and removed instances - " + Arrays.toString(
                removedInstances.toArray(new String[0])));

        TreeSet<InstanceTopicQueueHolder> orderedSet = new TreeSet<InstanceTopicQueueHolder>(InstanceTopicQueueHolder.getComparator());
        Set<MessageQueue> tpiNeedsToBeAssigned = new HashSet<MessageQueue>();

        tpiNeedsToBeAssigned.addAll(unassignedTopicPartitions);

        for (String instanceName : instanceToTopicPartitionMap.keySet()) {
            if (!removedInstances.contains(instanceName)) {
                InstanceTopicQueueHolder instance = new InstanceTopicQueueHolder(instanceName);
                instance.addTopicQueues(instanceToTopicPartitionMap.get(instanceName));
                orderedSet.add(instance);
            } else {
                tpiNeedsToBeAssigned.addAll(instanceToTopicPartitionMap.get(instanceName));
            }
        }

        for (String instanceName : newInstances) {
            orderedSet.add(new InstanceTopicQueueHolder(instanceName));
        }

        orderedSet.last().addTopicQueues(tpiNeedsToBeAssigned);
        Set<InstanceTopicQueueHolder> balanceAssignment = balanceAssignment(orderedSet);
        return balanceAssignment;
    }

    private static void assignIdealStates(HelixManager helixManager, Map<String, IdealState> idealStatesFromAssignment) {
        HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
        String helixClusterName = helixManager.getClusterName();
        for (String topic : idealStatesFromAssignment.keySet()) {
            IdealState idealState = idealStatesFromAssignment.get(topic);
            helixAdmin.setResourceIdealState(helixClusterName, topic, idealState);
        }
    }

    private static Set<InstanceTopicQueueHolder> balanceAssignment(TreeSet<InstanceTopicQueueHolder> orderedSet) {
        while (!isAssignmentBalanced(orderedSet)) {
            InstanceTopicQueueHolder lowestInstance = orderedSet.pollFirst();
            InstanceTopicQueueHolder highestInstance = orderedSet.pollLast();
            MessageQueue messageQueue = highestInstance.getServingTopicQueueSet().iterator().next();
            highestInstance.removeTopicQueue(messageQueue);
            lowestInstance.addTopicQueue(messageQueue);
            orderedSet.add(lowestInstance);
            orderedSet.add(highestInstance);
        }
        return orderedSet;
    }

    private static boolean isAssignmentBalanced(TreeSet<InstanceTopicQueueHolder> set) {
        return (set.last().getNumServingTopicQueues() - set.first().getNumServingTopicQueues() <= 1);
    }

    private static Set<String> getLiveInstanceName(List<LiveInstance> liveInstances) {
        Set<String> liveInstanceNames = new HashSet<String>();
        for (LiveInstance liveInstance : liveInstances) {
            liveInstanceNames.add(liveInstance.getInstanceName());
        }
        return liveInstanceNames;
    }

    private static Set<String> getAddedInstanceSet(Set<String> liveInstances, Set<String> currentInstances) {

        Set<String> addedInstances = new HashSet<String>();
        addedInstances.addAll(liveInstances);
        addedInstances.removeAll(currentInstances);
        return addedInstances;
    }

    private static Set<String> getRemovedInstanceSet(Set<String> liveInstances, Set<String> currentInstances) {
        Set<String> removedInstances = new HashSet<String>();
        removedInstances.addAll(currentInstances);
        removedInstances.removeAll(liveInstances);
        return removedInstances;
    }

}
