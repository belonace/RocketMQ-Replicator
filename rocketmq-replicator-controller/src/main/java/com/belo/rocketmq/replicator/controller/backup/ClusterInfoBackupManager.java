package com.belo.rocketmq.replicator.controller.backup;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.belo.rocketmq.replicator.controller.ControllerConf;
import com.belo.rocketmq.replicator.controller.core.HelixRocketmqReplicatorManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 集群信息备份管理
 * Created by liqiang on 2017/9/27.
 */
public class ClusterInfoBackupManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoBackupManager.class);

    private final HelixRocketmqReplicatorManager helixRocketmqReplicatorManager;

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    //一天保存一次
    private int timeValue = 24 * 60 * 60;
    private TimeUnit timeUnit = TimeUnit.SECONDS;

    private final BackUpHandler handler;
    private final ControllerConf config;

    public ClusterInfoBackupManager(HelixRocketmqReplicatorManager helixRocketmqReplicatorManager,
                                    BackUpHandler handler,
                                    ControllerConf config) {
        this.helixRocketmqReplicatorManager = helixRocketmqReplicatorManager;
        this.handler = handler;
        this.config = config;
    }

    public void start() {
        LOGGER.info("Trying to schedule cluster backup job at rate {} {} !", this.timeValue, this.timeUnit.toString());
        this.executorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    dumpState();
                } catch (Exception e) {
                    LOGGER.error(String.format("Failed to take backup, with exception: %s", e));
                }
                LOGGER.info("Backup taken successfully!");
            }
        }, 20, this.timeValue, this.timeUnit);
    }

    public synchronized void dumpState() throws Exception {
        if (!this.helixRocketmqReplicatorManager.isLeader()) {
            return;
        }

        LOGGER.info("Backing up the CurrentState and the IdealState!");
        StringBuilder idealState = new StringBuilder();
        StringBuilder partitionAssignment = new StringBuilder();
        List<String> topicLists = this.helixRocketmqReplicatorManager.getTopicLists();
        if (topicLists == null || topicLists.isEmpty()) {
            LOGGER.info("No topics available to take backup");
            return;
        }

        JSONArray resultList = new JSONArray();

        for (String topicName : topicLists) {
            IdealState idealStateForTopic = this.helixRocketmqReplicatorManager.getIdealStateForTopic(topicName);
            JSONObject resultJson = new JSONObject();
            resultJson.put("topic", topicName);
            resultJson.put("idealStateMeta", idealStateForTopic);
            resultList.add(resultJson);
        }

        idealState.append(resultList.toJSONString());

        resultList = new JSONArray();

        for (String topicName : topicLists) {
            IdealState idealStateForTopic = this.helixRocketmqReplicatorManager.getIdealStateForTopic(topicName);
            ExternalView externalViewForTopic = this.helixRocketmqReplicatorManager.getExternalViewForTopic(topicName);
            JSONObject resultJson = new JSONObject();
            resultJson.put("topic", topicName);
            JSONObject externalViewPartitionToServerMappingJson = new JSONObject();
            if (externalViewForTopic != null) {
                for (String partition : externalViewForTopic.getPartitionSet()) {
                    Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
                    for (String server : stateMap.keySet()) {
                        if (!externalViewPartitionToServerMappingJson.containsKey(partition)) {
                            externalViewPartitionToServerMappingJson.put(partition, new JSONArray());
                        }
                        externalViewPartitionToServerMappingJson.getJSONArray(partition).add(server);
                    }
                }
            }

            resultJson.put("externalView", externalViewPartitionToServerMappingJson);

            JSONObject idealStatePartitionToServerMappingJson = new JSONObject();
            for (String partition : idealStateForTopic.getPartitionSet()) {
                Map<String, String> stateMap = idealStateForTopic.getInstanceStateMap(partition);
                if (stateMap != null) {
                    for (String server : stateMap.keySet()) {
                        if (!idealStatePartitionToServerMappingJson.containsKey(partition)) {
                            idealStatePartitionToServerMappingJson.put(partition, new JSONArray());
                        }
                        idealStatePartitionToServerMappingJson.getJSONArray(partition).add(server);
                    }
                }
            }

            resultJson.put("idealStateMapping", idealStatePartitionToServerMappingJson);
            Map<String, List<String>> serverToPartitionMapping = new HashMap<String, List<String>>();
            JSONObject serverToPartitionMappingJson = new JSONObject();
            JSONObject serverToNumPartitionsMappingJson = new JSONObject();

            if (externalViewForTopic != null) {
                for (String partition : externalViewForTopic.getPartitionSet()) {
                    Map<String, String> stateMap = externalViewForTopic.getStateMap(partition);
                    for (String server : stateMap.keySet()) {
                        if (stateMap.get(server).equals("ONLINE")) {
                            if (!serverToPartitionMapping.containsKey(server)) {
                                serverToPartitionMapping.put(server, new ArrayList<String>());
                                serverToPartitionMappingJson.put(server, new JSONArray());
                                serverToNumPartitionsMappingJson.put(server, 0);
                            }
                            serverToPartitionMapping.get(server).add(partition);
                            serverToPartitionMappingJson.getJSONArray(server).add(partition);
                            serverToNumPartitionsMappingJson.put(server, serverToNumPartitionsMappingJson.getInteger(server) + 1);
                        }
                    }
                }
            }
            resultJson.put("serverToPartitionMapping", serverToPartitionMappingJson);
            resultJson.put("serverToNumPartitionsMapping", serverToNumPartitionsMappingJson);
            resultList.add(resultJson);
        }

        partitionAssignment.append(resultList.toJSONString());

        String idealStateFileName = "idealState-backup-" + ControllerConf.DataId;
        String paritionAssignmentFileName = "partitionAssgn-backup-" + ControllerConf.DataId;
        try {
            this.handler.writeToFile(idealStateFileName, idealState.toString());
            this.handler.writeToFile(paritionAssignmentFileName, partitionAssignment.toString());
        } catch (Exception e) {
            throw e;
        }
    }
}
