package com.belo.rocketmq.replicator.worker;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liqiang on 2017/10/11.
 */
public class OnlineOfflineStateModel extends StateModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineOfflineStateModel.class);

    private String instanceId;

    private RocketmqConnector rocketmqConnector;

    public static final String name = "OnlineOffline";

    public OnlineOfflineStateModel(String instanceId, RocketmqConnector rocketmqConnector){
        this.instanceId = instanceId;
        this.rocketmqConnector = rocketmqConnector;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext notificationContext){
        LOGGER.info("OnlineOfflineStateModel.onBecomeOnlineFromOffline for topic: "
                + message.getResourceName() + ", partition: " + message.getPartitionName()
                + " to instance: " + instanceId);

        // add topic partition on the instance
        this.rocketmqConnector.addTopicMessageQueue(message.getResourceName(), message.getPartitionName());

        LOGGER.debug("Finish OnlineOfflineStateModel.onBecomeOnlineFromOffline for topic: "
                + message.getResourceName() + ", partition: " + message.getPartitionName()
                + " to instance: " + instanceId);
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext notificationContext) {
        LOGGER.info("OnlineOfflineStateModel.onBecomeOfflineFromOnline for topic: "
                + message.getResourceName() + ", partition: " + message.getPartitionName()
                + " to instance: " + instanceId);

        // delete topic partition on the instance
        this.rocketmqConnector.deleteTopicMessageQueue(message.getResourceName(), message.getPartitionName());

        LOGGER.debug("Finish OnlineOfflineStateModel.onBecomeOfflineFromOnline for topic: "
                + message.getResourceName() + ", partition: " + message.getPartitionName()
                + " to instance: " + instanceId);
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext notificationContext) {
        LOGGER.info("OnlineOfflineStateModel.onBecomeDroppedFromOffline for topic: "
                + message.getResourceName() + ", partition: " + message.getPartitionName()
                + " to instance: " + instanceId);
        // do nothing
    }
}
