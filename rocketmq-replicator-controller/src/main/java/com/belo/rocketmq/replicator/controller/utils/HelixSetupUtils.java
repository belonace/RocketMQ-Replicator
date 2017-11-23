package com.belo.rocketmq.replicator.controller.utils;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liqiang on 2017/9/27.
 */
public class HelixSetupUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(HelixSetupUtils.class);

    public static synchronized HelixManager setup(String helixClusterName, String zkPath, String controllerInstanceId) {
        try {
            createHelixClusterIfNeeded(helixClusterName, zkPath);
        } catch (final Exception e) {
            LOGGER.error("Caught exception", e);
            return null;
        }

        try {
            return startHelixControllerInStandadloneMode(helixClusterName, zkPath, controllerInstanceId);
        } catch (final Exception e) {
            LOGGER.error("Caught exception", e);
            return null;
        }
    }

    public static void createHelixClusterIfNeeded(String helixClusterName, String zkPath) {
        final HelixAdmin admin = new ZKHelixAdmin(zkPath);

        if (admin.getClusters().contains(helixClusterName)) {
            LOGGER.info("cluster already exist, skipping it.. ********************************************* ");
            return;
        }

        LOGGER.info("Creating a new cluster, as the helix cluster : " + helixClusterName
                + " was not found ********************************************* ");
        admin.addCluster(helixClusterName, false);

        LOGGER.info("Enable mirror maker machines auto join.");
        final HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER)
                .forCluster(helixClusterName).build();

        final Map<String, String> props = new HashMap<String, String>();
        props.put(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, String.valueOf(true));
        props.put(MessageType.STATE_TRANSITION + "." + HelixTaskExecutor.MAX_THREADS,
                String.valueOf(100));

        admin.setConfig(scope, props);

        LOGGER.info("Adding state model definition named : OnlineOffline generated using : "
                + OnlineOfflineStateModel.class.toString()
                + " ********************************************** ");

        // add state model definition
        admin.addStateModelDef(helixClusterName, OnlineOfflineStateModel.name, OnlineOfflineStateModel.build());

        LOGGER.info("New Cluster setup completed... ********************************************** ");
    }

    private static HelixManager startHelixControllerInStandadloneMode(String helixClusterName, String zkUrl, String controllerInstanceId) {
        LOGGER.info("Starting Helix Standalone Controller ... ");
        return HelixControllerMain.startHelixController(zkUrl, helixClusterName, controllerInstanceId, HelixControllerMain.STANDALONE);
    }

    /**
     * TODO 集群的需要测试
     * @param helixClusterName
     * @param zkUrl
     * @param controllerInstanceId
     * @return
     */
    private static HelixManager startHelixControllerInDistributedMode(String helixClusterName, String zkUrl, String controllerInstanceId) {
        LOGGER.info("Starting Helix Standalone Controller ... ");
        return HelixControllerMain.startHelixController(zkUrl, helixClusterName, controllerInstanceId, HelixControllerMain.DISTRIBUTED);
    }
}
