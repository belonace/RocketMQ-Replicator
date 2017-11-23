package com.belo.rocketmq.replicator.controller;

import com.alibaba.fastjson.JSON;
import com.belo.rocketmq.replicator.controller.utils.PropertyConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by liqiang on 2017/9/27.
 */
public class ControllerConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(ControllerConf.class);

    private ControllerConf(){}

    /**
     * helix zk地址
     */
    private String zkStr;

    /**
     * helix集群名称
     */
    private String helixClusterName;

    /**
     * controller id
     */
    private String instanceId;

    /**
     * helix 平衡时间
     */
    private int autoRebalanceDelayInSeconds = 120;

    /**
     * 本地保存备份路径
     */
    private String localBackupFilePath;

    /**
     * 源MQ nameserver地址
     */
    private String srcRocketMQPath;

    /**
     * 目标mq nameserver地址
     */
    private String destRocketMQPath;

    /**
     * 白名单启用
     */
    private boolean enableAutoWhitelist = true;

    /**
     * 白名单更新周期
     */
    private int refreshTimeInSeconds = 60;

    /**
     * 白名单起始时间
     */
    private int initWaitTimeInSeconds = 5;

    /**
     * 白名单，且必须配置
     */
    private String writeList;

    public String getZkStr() {
        return zkStr;
    }

    public void setZkStr(String zkStr) {
        this.zkStr = zkStr;
    }

    public String getHelixClusterName() {
        return helixClusterName;
    }

    public void setHelixClusterName(String helixClusterName) {
        this.helixClusterName = helixClusterName;
    }

    public String getInstanceId() {
        if (StringUtils.isBlank(instanceId)) {
            try {
                setInstanceId(InetAddress.getLocalHost().getHostName());
            } catch (UnknownHostException e) {
                setInstanceId("RocketMQ-Controller");
            }
        }
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public int getAutoRebalanceDelayInSeconds() {
        return autoRebalanceDelayInSeconds;
    }

    public void setAutoRebalanceDelayInSeconds(int autoRebalanceDelayInSeconds) {
        this.autoRebalanceDelayInSeconds = autoRebalanceDelayInSeconds;
    }

    public static ControllerConf getControllerConf() throws Exception {
        ControllerConf controllerConf = new ControllerConf();
        controllerConf.setAutoRebalanceDelayInSeconds(Integer.parseInt(PropertyConfig.getProperties().getProperty("AutoRebalanceDelayInSeconds")));
        controllerConf.setDestRocketMQPath(PropertyConfig.getProperties().getProperty("DestRocketMQPath"));
        controllerConf.setEnableAutoWhitelist(Boolean.parseBoolean(PropertyConfig.getProperties().getProperty("EnableAutoWhitelist")));
        controllerConf.setHelixClusterName(PropertyConfig.getProperties().getProperty("HelixClusterName"));
        controllerConf.setInitWaitTimeInSeconds(Integer.parseInt(PropertyConfig.getProperties().getProperty("InitWaitTimeInSeconds")));
        controllerConf.setInstanceId(PropertyConfig.getProperties().getProperty("InstanceId"));
        controllerConf.setLocalBackupFilePath(PropertyConfig.getProperties().getProperty("LocalBackupFilePath"));
        controllerConf.setRefreshTimeInSeconds(Integer.parseInt(PropertyConfig.getProperties().getProperty("RefreshTimeInSeconds")));
        controllerConf.setSrcRocketMQPath(PropertyConfig.getProperties().getProperty("SrcRocketMQPath"));
        controllerConf.setZkStr(PropertyConfig.getProperties().getProperty("ZkStr"));
        controllerConf.setWriteList(PropertyConfig.getProperties().getProperty("WriteList"));

        return controllerConf;
    }

    private static void changeInstanceValue(ControllerConf controllerConf, String configInfo) {
        ControllerConf newCont = JSON.parseObject(configInfo, ControllerConf.class);
        controllerConf.setAutoRebalanceDelayInSeconds(newCont.getAutoRebalanceDelayInSeconds());
        controllerConf.setDestRocketMQPath(newCont.getDestRocketMQPath());
        controllerConf.setEnableAutoWhitelist(newCont.isEnableAutoWhitelist());
        controllerConf.setHelixClusterName(newCont.getHelixClusterName());
        controllerConf.setInitWaitTimeInSeconds(newCont.getInitWaitTimeInSeconds());
        controllerConf.setInstanceId(newCont.getInstanceId());
        controllerConf.setLocalBackupFilePath(newCont.getLocalBackupFilePath());
        controllerConf.setRefreshTimeInSeconds(newCont.getRefreshTimeInSeconds());
        controllerConf.setSrcRocketMQPath(newCont.getSrcRocketMQPath());
        controllerConf.setZkStr(newCont.getZkStr());
        controllerConf.setWriteList(newCont.getWriteList());
    }

    public String getLocalBackupFilePath() {
        return this.localBackupFilePath;
    }

    public void setLocalBackupFilePath(String localBackupFilePath) {
        this.localBackupFilePath = localBackupFilePath;
    }

    public String getSrcRocketMQPath() {
        return this.srcRocketMQPath;
    }

    public String getDestRocketMQPath() {
        return this.destRocketMQPath;
    }

    public void setSrcRocketMQPath(String srcRocketMQPath) {
        this.srcRocketMQPath = srcRocketMQPath;
    }

    public void setDestRocketMQPath(String destRocketMQPath) {
        this.destRocketMQPath = destRocketMQPath;
    }

    public boolean isEnableAutoWhitelist() {
        return enableAutoWhitelist;
    }

    public void setEnableAutoWhitelist(boolean enableAutoWhitelist) {
        this.enableAutoWhitelist = enableAutoWhitelist;
    }

    public int getRefreshTimeInSeconds() {
        return refreshTimeInSeconds;
    }

    public void setRefreshTimeInSeconds(int refreshTimeInSeconds) {
        this.refreshTimeInSeconds = refreshTimeInSeconds;
    }

    public int getInitWaitTimeInSeconds() {
        return initWaitTimeInSeconds;
    }

    public void setInitWaitTimeInSeconds(int initWaitTimeInSeconds) {
        this.initWaitTimeInSeconds = initWaitTimeInSeconds;
    }

    public boolean checkWorkerConf() {

        if(StringUtils.isEmpty(this.getZkStr())){
            LOGGER.info("ZkStr is Empty");
            return false;
        }

        if(StringUtils.isEmpty(this.getHelixClusterName())){
            LOGGER.info("HelixClusterName is Empty");
            return false;
        }

        if(StringUtils.isEmpty(this.getInstanceId())){
            LOGGER.info("InstanceId is Empty");
            return false;
        }

        if(StringUtils.isEmpty(this.getSrcRocketMQPath())){
            LOGGER.info("SrcRocketMQPath is Empty");
            return false;
        }

        if(StringUtils.isEmpty(this.getDestRocketMQPath())){
            LOGGER.info("DestRocketMQPath is Empty");
            return false;
        }

        if(StringUtils.isEmpty(this.getLocalBackupFilePath())){
            LOGGER.info("LocalBackupFilePath is Empty");
            return false;
        }

        if(StringUtils.isEmpty(this.getWriteList())){
            LOGGER.info("WriteList is Empty");
            return false;
        }

        return true;
    }

    public String getWriteList() {
        return writeList;
    }

    public void setWriteList(String writeList) {
        this.writeList = writeList;
    }

    @Override
    public String toString() {
        return "ControllerConf{" +
                "zkStr='" + zkStr + '\'' +
                ", helixClusterName='" + helixClusterName + '\'' +
                ", instanceId='" + instanceId + '\'' +
                ", autoRebalanceDelayInSeconds=" + autoRebalanceDelayInSeconds +
                ", localBackupFilePath='" + localBackupFilePath + '\'' +
                ", srcRocketMQPath='" + srcRocketMQPath + '\'' +
                ", destRocketMQPath='" + destRocketMQPath + '\'' +
                ", enableAutoWhitelist=" + enableAutoWhitelist +
                ", refreshTimeInSeconds=" + refreshTimeInSeconds +
                ", initWaitTimeInSeconds=" + initWaitTimeInSeconds +
                ", writeList='" + writeList + '\'' +
                '}';
    }
}
