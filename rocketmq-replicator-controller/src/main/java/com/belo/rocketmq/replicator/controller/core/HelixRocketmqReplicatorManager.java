package com.belo.rocketmq.replicator.controller.core;

import com.belo.rocketmq.replicator.controller.ControllerConf;
import com.belo.rocketmq.replicator.controller.utils.HelixSetupUtils;
import com.belo.rocketmq.replicator.controller.utils.HelixUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Helix Controller 主要逻辑. 提供所有关于topic的接口
 * 有两种模式 auto/custom:模式介绍如下：
 * Auto mode is for helix taking care of all the idealStates changes
 * Custom mode is for creating a balanced idealStates when necessary,
 *
 * node的增加和移除, Topic的增加。扩展和移除
 *
 * Created by liqiang on 2017/9/28.
 */
public class HelixRocketmqReplicatorManager {

    private static final String ENABLE = "enable";
    private static final String DISABLE = "disable";

    //自动模式
    private static final String AUTO_BALANCING = "AutoBalancing";

    private static final Logger LOGGER = LoggerFactory.getLogger(HelixRocketmqReplicatorManager.class);

    private final ControllerConf controllerConf;
    private final String helixClusterName;
    private final String helixZkURL;
    private HelixManager helixZkManager;
    private HelixAdmin helixAdmin;
    private String instanceId;
    private RocketmqBrokerTopicObserver srcBrokerTopicObserver;

    private final PriorityQueue<InstanceTopicQueueHolder> currentServingInstance =
            new PriorityQueue<InstanceTopicQueueHolder>(100, InstanceTopicQueueHolder.getComparator());

    public HelixRocketmqReplicatorManager(ControllerConf controllerConf, RocketmqBrokerTopicObserver srcBrokerTopicObserver) {
        this.controllerConf = controllerConf;
        this.helixZkURL = HelixUtils.getAbsoluteZkPathForHelix(this.controllerConf.getZkStr());
        this.helixClusterName = this.controllerConf.getHelixClusterName();
        this.instanceId = controllerConf.getInstanceId();
        this.srcBrokerTopicObserver = srcBrokerTopicObserver;
    }

    public void start() {
        LOGGER.info("Trying to start HelixMirrorMakerManager!");

        this.helixZkManager = HelixSetupUtils.setup(this.helixClusterName, this.helixZkURL, this.instanceId);

        this.helixAdmin = this.helixZkManager.getClusterManagmentTool();

        LOGGER.info("Trying to register AutoRebalanceLiveInstanceChangeListener");

        AutoRebalanceLiveInstanceChangeListener autoRebalanceLiveInstanceChangeListener =
                new AutoRebalanceLiveInstanceChangeListener(this, this.helixZkManager,
                        this.controllerConf.getAutoRebalanceDelayInSeconds(), this.srcBrokerTopicObserver);

        updateCurrentServingInstance();

        try {
            this.helixZkManager.addLiveInstanceChangeListener(autoRebalanceLiveInstanceChangeListener);
        } catch (Exception e) {
            LOGGER.error("Failed to add LiveInstanceChangeListener");
        }
    }

    public void stop() {
        LOGGER.info("Trying to stop HelixMirrorMakerManager!");
        this.helixZkManager.disconnect();
    }

    public synchronized void updateCurrentServingInstance() {
        synchronized (this.currentServingInstance) {
            Map<String, InstanceTopicQueueHolder> instanceMap = new HashMap<String, InstanceTopicQueueHolder>();
            Map<String, Set<MessageQueue>> instanceToTopicPartitionsMap = HelixUtils.getInstanceToTopicQueuesMap(this.helixZkManager);

            List<String> liveInstances = HelixUtils.liveInstances(this.helixZkManager);

            for (String instanceName : liveInstances) {
                InstanceTopicQueueHolder instance = new InstanceTopicQueueHolder(instanceName);
                instanceMap.put(instanceName, instance);
            }

            for (String instanceName : instanceToTopicPartitionsMap.keySet()) {
                if (instanceMap.containsKey(instanceName)) {
                    instanceMap.get(instanceName).addTopicQueues(instanceToTopicPartitionsMap.get(instanceName));
                }
            }

            this.currentServingInstance.clear();
            this.currentServingInstance.addAll(instanceMap.values());
        }
    }

    public synchronized void addTopicToMirrorMaker(String topicName, TopicPublishInfo topicPublishInfo) {
        setEmptyResourceConfig(topicName);
        updateCurrentServingInstance();
        synchronized (this.currentServingInstance) {
            this.helixAdmin.addResource(this.helixClusterName, topicName,
                    IdealStateBuilder.buildCustomIdealStateFor(topicName, topicPublishInfo, this.currentServingInstance));

            //ExternalView externalView = this.helixAdmin.getResourceExternalView(this.helixClusterName, topicName);
        }
    }

    private synchronized void setEmptyResourceConfig(String topicName) {
        this.helixAdmin.setConfig(new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(this.helixClusterName)
                        .forResource(topicName).build(), new HashMap<String, String>());
    }

    public synchronized void expandTopicInMirrorMaker(String topicName, TopicPublishInfo topicPublishInfo) {
        updateCurrentServingInstance();
        synchronized (this.currentServingInstance) {
            this.helixAdmin.setResourceIdealState(this.helixClusterName, topicName,
                    IdealStateBuilder.expandCustomRebalanceModeIdealStateFor(
                            this.helixAdmin.getResourceIdealState(this.helixClusterName, topicName), topicName,
                            topicPublishInfo,
                            this.currentServingInstance));

            //List<String> resourceList = new ArrayList<String>(5);
            //resourceList.add(topicName);
            //this.helixAdmin.resetResource(this.helixClusterName, resourceList);
        }
    }

    public synchronized void deleteTopicInMirrorMaker(String topicName) {
        this.helixAdmin.dropResource(this.helixClusterName, topicName);
    }

    public IdealState getIdealStateForTopic(String topicName) {
        return this.helixAdmin.getResourceIdealState(this.helixClusterName, topicName);
    }

    public ExternalView getExternalViewForTopic(String topicName) {
        return this.helixAdmin.getResourceExternalView(this.helixClusterName, topicName);
    }

    public boolean isTopicExisted(String topicName) {
        return this.helixAdmin.getResourcesInCluster(this.helixClusterName).contains(topicName);
    }

    public List<String> getTopicLists() {
        return this.helixAdmin.getResourcesInCluster(this.helixClusterName);
    }

    public boolean isAutoBalancingEnabled() {
        HelixConfigScope scope = new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(this.helixClusterName).build();
        Map<String, String> config = this.helixAdmin.getConfig(scope, Arrays.asList(AUTO_BALANCING));
        if (config.containsKey(AUTO_BALANCING) && config.get(AUTO_BALANCING).equals(DISABLE)) {
            return false;
        }
        return true;
    }

    public boolean isLeader() {
        return this.helixZkManager.isLeader();
    }

    public List<LiveInstance> getCurrentLiveInstances() {
        HelixDataAccessor helixDataAccessor = this.helixZkManager.getHelixDataAccessor();
        PropertyKey liveInstancePropertyKey = new Builder(this.helixClusterName).liveInstances();
        List<LiveInstance> liveInstances = helixDataAccessor.getChildValues(liveInstancePropertyKey);
        return liveInstances;
    }

}
