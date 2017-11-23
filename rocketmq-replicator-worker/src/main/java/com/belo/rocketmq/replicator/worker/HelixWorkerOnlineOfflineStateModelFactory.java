package com.belo.rocketmq.replicator.worker;

import org.apache.helix.participant.statemachine.StateModelFactory;

/**
 * Created by liqiang on 2017/10/11.
 */
public class HelixWorkerOnlineOfflineStateModelFactory extends StateModelFactory<OnlineOfflineStateModel> {

    private String instanceId;

    private RocketmqConnector rocketmqConnector;


    public HelixWorkerOnlineOfflineStateModelFactory(String instanceId, RocketmqConnector rocketmqConnector) {
        this.instanceId = instanceId;
        this.rocketmqConnector = rocketmqConnector;
    }

    @Override
    public OnlineOfflineStateModel createNewStateModel(String resourceName, String partitionName) {
        return new OnlineOfflineStateModel(this.instanceId, this.rocketmqConnector);
    }

}
