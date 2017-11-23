package com.belo.rocketmq.replicator.controller.core;

import com.belo.rocketmq.replicator.controller.utils.ControllerConst;
import com.belo.rocketmq.replicator.controller.utils.OnlineOfflineStateModel;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;

/**
 * 处理增加或者扩容topic的idealStates变化,idealStates的创建，集群Controller基本按照客户端的算法来平衡states
 * Created by liqiang on 2017/9/28.
 */
public class IdealStateBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(IdealStateBuilder.class);

    /**
     * 新增topic的IdealState，逻辑为平均分配
     *
     * @param topicName
     * @param topicPublishInfo
     * @param instanceToNumServingTopicPartitionMap
     * @return
     */
    public static IdealState buildCustomIdealStateFor(String topicName, TopicPublishInfo topicPublishInfo,
                                                      PriorityQueue<InstanceTopicQueueHolder> instanceToNumServingTopicPartitionMap) {

        final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

        customModeIdealStateBuilder.setStateModel(OnlineOfflineStateModel.name).setNumReplica(1);

        for (MessageQueue messageQueue : topicPublishInfo.getMessageQueueList()) {
            InstanceTopicQueueHolder liveInstance = instanceToNumServingTopicPartitionMap.poll();
            if (liveInstance != null) {

                customModeIdealStateBuilder.assignInstanceAndState(messageQueue.getBrokerName() + ControllerConst.SEPARATOR + messageQueue.getQueueId(),
                        liveInstance.getInstanceName(), "ONLINE");

                liveInstance.addTopicQueue(messageQueue);
                instanceToNumServingTopicPartitionMap.add(liveInstance);
            }
        }

        IdealState idealState = customModeIdealStateBuilder.build();
        idealState.setNumPartitions(topicPublishInfo.getMessageQueueList().size());
        idealState.setMaxPartitionsPerInstance(Integer.MAX_VALUE);

        return idealState;
    }

    /**
     * 扩展的需求是要达到老的queue的所在实例不动，新增的queue按照顺序添加到实例
     *
     * @param oldIdealState
     * @param topicName
     * @param topicPublishInfo
     * @param instanceToNumServingTopicPartitionMap
     * @return
     */
    public static IdealState expandCustomRebalanceModeIdealStateFor(IdealState oldIdealState,
                                                                    String topicName, TopicPublishInfo topicPublishInfo,
                                                                    PriorityQueue<InstanceTopicQueueHolder> instanceToNumServingTopicPartitionMap) {

        final CustomModeISBuilder customModeIdealStateBuilder = new CustomModeISBuilder(topicName);

        customModeIdealStateBuilder.setStateModel(OnlineOfflineStateModel.name).setNumReplica(1);

        for (MessageQueue messageQueue : topicPublishInfo.getMessageQueueList()) {

            String queueName = messageQueue.getBrokerName() + ControllerConst.SEPARATOR + messageQueue.getQueueId();

            if (oldIdealState.getInstanceSet(queueName) == null || oldIdealState.getInstanceSet(queueName).size() == 0) {
                InstanceTopicQueueHolder liveInstance = instanceToNumServingTopicPartitionMap.poll();

                if (liveInstance != null) {
                    customModeIdealStateBuilder.assignInstanceAndState(queueName, liveInstance.getInstanceName(), "ONLINE");

                    liveInstance.addTopicQueue(messageQueue);
                    instanceToNumServingTopicPartitionMap.add(liveInstance);
                }
            } else {
                try {
                    String instanceName = oldIdealState.getInstanceStateMap(queueName).keySet().iterator().next();
                    customModeIdealStateBuilder.assignInstanceAndState(queueName, instanceName, "ONLINE");
                } catch (Exception e) {
                    LOGGER.error("expandCustomRebalanceModeIdealStateFor error : {}", e);
                }
            }

        }

        IdealState idealState = customModeIdealStateBuilder.build();
        idealState.setNumPartitions(topicPublishInfo.getMessageQueueList().size());
        idealState.setMaxPartitionsPerInstance(Integer.MAX_VALUE);

        return idealState;
    }

}
