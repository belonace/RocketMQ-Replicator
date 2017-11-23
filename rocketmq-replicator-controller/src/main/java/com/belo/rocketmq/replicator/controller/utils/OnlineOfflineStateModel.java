package com.belo.rocketmq.replicator.controller.utils;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helix 内置 Online-offline 状态模型定义
 * Created by liqiang on 2017/9/27.
 */
public final class OnlineOfflineStateModel {

    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineOfflineStateModel.class);

    public static final String name = "OnlineOffline";

    public enum States {
        ONLINE,
        OFFLINE
    }

    /**
     * Build OnlineOffline state model definition
     */
    public static StateModelDefinition build() {
        StateModelDefinition.Builder builder = new StateModelDefinition.Builder(name);
        // init state
        builder.initialState(States.OFFLINE.name());

        // add states
        builder.addState(States.ONLINE.name(), 20);
        builder.addState(States.OFFLINE.name(), -1);

        for (HelixDefinedState state : HelixDefinedState.values()) {
            builder.addState(state.name(), -1);
        }

        // add transitions

        builder.addTransition(States.ONLINE.name(), States.OFFLINE.name(), 25);
        builder.addTransition(States.OFFLINE.name(), States.ONLINE.name(), 5);
        builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name(), 0);

        // bounds
        builder.dynamicUpperBound(States.ONLINE.name(), "R");

        return builder.build();
    }

}
