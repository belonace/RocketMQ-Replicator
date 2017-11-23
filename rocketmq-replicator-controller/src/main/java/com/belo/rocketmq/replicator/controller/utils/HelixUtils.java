package com.belo.rocketmq.replicator.controller.utils;

import com.belo.rocketmq.replicator.controller.core.InstanceTopicQueueHolder;
import com.belo.rocketmq.replicator.controller.core.RocketmqBrokerTopicObserver;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;

/**
 * Created by liqiang on 2017/9/27.
 */
public class HelixUtils {

    public static String getAbsoluteZkPathForHelix(String zkBaseUrl) {
        zkBaseUrl = StringUtils.chomp(zkBaseUrl, "/");
        return zkBaseUrl;
    }

    public static List<String> liveInstances(HelixManager helixManager) {
        HelixDataAccessor helixDataAccessor = helixManager.getHelixDataAccessor();
        PropertyKey liveInstancesKey = helixDataAccessor.keyBuilder().liveInstances();
        return ImmutableList.copyOf(helixDataAccessor.getChildNames(liveInstancesKey));
    }

    /**
     * 获取Controller上实例和queue的对应关系
     *
     * @return InstanceToNumTopicQueueMap
     */
    public static Map<String, Set<MessageQueue>> getInstanceToTopicQueuesMap(HelixManager helixManager) {
        Map<String, Set<MessageQueue>> instanceToNumTopicQueueMap = new HashMap<String, Set<MessageQueue>>();
        HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
        String helixClusterName = helixManager.getClusterName();

        for (String topic : helixAdmin.getResourcesInCluster(helixClusterName)) {
            IdealState is = helixAdmin.getResourceIdealState(helixClusterName, topic);
            for (String partition : is.getPartitionSet()) {
                String[] queueinfo = partition.split(ControllerConst.SEPARATOR);
                MessageQueue messageQueue = new MessageQueue(topic, queueinfo[0], Integer.parseInt(queueinfo[1]));
                for (String instance : is.getInstanceSet(partition)) {
                    if (!instanceToNumTopicQueueMap.containsKey(instance)) {
                        instanceToNumTopicQueueMap.put(instance, new HashSet<MessageQueue>());
                    }
                    instanceToNumTopicQueueMap.get(instance).add(messageQueue);
                }
            }
        }
        return instanceToNumTopicQueueMap;
    }

    /**
     * 根据新值，重新生成topic维度的IdealState
     * @param newAssignment
     * @return
     */
    public static Map<String, IdealState> getIdealStatesFromAssignment(Set<InstanceTopicQueueHolder> newAssignment) {
        Map<String, CustomModeISBuilder> idealStatesBuilderMap = new HashMap<String, CustomModeISBuilder>();

        for (InstanceTopicQueueHolder instance : newAssignment) {
            for (MessageQueue messageQueue : instance.getServingTopicQueueSet()) {
                String topicName = messageQueue.getTopic();
                String partition = messageQueue.getBrokerName() + ControllerConst.SEPARATOR + messageQueue.getQueueId();

                if (!idealStatesBuilderMap.containsKey(topicName)) {
                    final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);
                    customModeIdealStateBuilder.setStateModel(OnlineOfflineStateModel.name).setNumReplica(1);

                    idealStatesBuilderMap.put(topicName, customModeIdealStateBuilder);
                }
                idealStatesBuilderMap.get(topicName).assignInstanceAndState(partition, instance.getInstanceName(), "ONLINE");
            }
        }

        Map<String, IdealState> idealStatesMap = new HashMap<String, IdealState>();

        for (String topic : idealStatesBuilderMap.keySet()) {
            IdealState idealState = idealStatesBuilderMap.get(topic).build();
            idealState.setMaxPartitionsPerInstance(Integer.MAX_VALUE);
            idealState.setNumPartitions(idealState.getPartitionSet().size());
            idealStatesMap.put(topic, idealState);
        }
        return idealStatesMap;
    }

    /**
     * 得到未分配的queue列表，之前在这里有个坑，现在已经修改
     * @param helixManager
     * @param srcBrokerTopicObserver
     * @return
     */
    public static Set<MessageQueue> getUnassignedQueues(HelixManager helixManager, RocketmqBrokerTopicObserver srcBrokerTopicObserver) {
        Set<MessageQueue> unassignedQueues = new HashSet<MessageQueue>();
        HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
        String helixClusterName = helixManager.getClusterName();

        for (String topic : helixAdmin.getResourcesInCluster(helixClusterName)) {
            IdealState is = helixAdmin.getResourceIdealState(helixClusterName, topic);

            for(MessageQueue messageQueue : srcBrokerTopicObserver.getTopicPartition(topic).getMessageQueueList()){
                if (is.getInstanceSet(messageQueue.getBrokerName() + ControllerConst.SEPARATOR + messageQueue.getQueueId()).isEmpty()) {
                    unassignedQueues.add(messageQueue);
                }
            }
        }
        return unassignedQueues;
    }

}
