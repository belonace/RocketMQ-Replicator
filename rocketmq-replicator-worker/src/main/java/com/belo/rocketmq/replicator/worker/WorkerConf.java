package com.belo.rocketmq.replicator.worker;

import com.alibaba.fastjson.JSON;
import com.belo.rocketmq.replicator.worker.utils.PropertyConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by liqiang on 2017/9/27.
 */
public class WorkerConf {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerConf.class);

    private WorkerConf() {
    }

    /**
     * helix zk地址
     */
    private String zkStr;

    /**
     * helix集群名称
     */
    private String helixClusterName;

    /**
     * worker id
     */
    private String workerInstanceId;

    /**
     * 源nameserver地址，用于消费者
     */
    private String srcRocketMQPath;

    /**
     * 目标nameserver地址，用于发送者
     */
    private String destRocketMQPath;

    /**
     * 一次拉取的最大消息数量
     */
    private int pullMaxNums = 64;

    /**
     * 消费线程数量
     */
    private int numConsumerPullThreads = 4;

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

    public String getWorkerInstanceId() {
        return workerInstanceId;
    }

    public static WorkerConf getWorkerConf() throws Exception {
        WorkerConf workerConf = new WorkerConf();
        workerConf.setZkStr(PropertyConfig.getProperties().getProperty("zkStr"));
        workerConf.setSrcRocketMQPath(PropertyConfig.getProperties().getProperty("srcRocketMQPath"));
        workerConf.setDestRocketMQPath(PropertyConfig.getProperties().getProperty("destRocketMQPath"));
        workerConf.setHelixClusterName(PropertyConfig.getProperties().getProperty("helixClusterName"));
        workerConf.setPullMaxNums(Integer.parseInt(PropertyConfig.getProperties().getProperty("pullMaxNums")));
        workerConf.setWorkerInstanceId(PropertyConfig.getProperties().getProperty("workerInstanceId"));
        workerConf.setNumConsumerPullThreads(Integer.parseInt(PropertyConfig.getProperties().getProperty("numConsumerPullThreads")));

        return workerConf;
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

    public boolean checkWorkerConf() {

        if (StringUtils.isEmpty(this.getZkStr())) {
            LOGGER.info("ZkStr is Empty");
            return false;
        }

        if (StringUtils.isEmpty(this.getHelixClusterName())) {
            LOGGER.info("HelixClusterName is Empty");
            return false;
        }

        if (StringUtils.isEmpty(this.getWorkerInstanceId())) {
            LOGGER.info("WorkerInstanceId is Empty");
            return false;
        }

        if (StringUtils.isEmpty(this.getSrcRocketMQPath())) {
            LOGGER.info("SrcRocketMQPath is Empty");
            return false;
        }

        if (StringUtils.isEmpty(this.getDestRocketMQPath())) {
            LOGGER.info("DestRocketMQPath is Empty");
            return false;
        }

        return true;
    }

    public void setWorkerInstanceId(String workerInstanceId) {
        this.workerInstanceId = workerInstanceId;
    }

    public int getPullMaxNums() {
        return pullMaxNums;
    }

    public void setPullMaxNums(int pullMaxNums) {
        this.pullMaxNums = pullMaxNums;
    }

    public int getNumConsumerPullThreads() {
        return numConsumerPullThreads;
    }

    public void setNumConsumerPullThreads(int numConsumerPullThreads) {
        this.numConsumerPullThreads = numConsumerPullThreads;
    }

    @Override
    public String toString() {
        return "WorkerConf{" +
                "zkStr='" + zkStr + '\'' +
                ", helixClusterName='" + helixClusterName + '\'' +
                ", workerInstanceId='" + workerInstanceId + '\'' +
                ", srcRocketMQPath='" + srcRocketMQPath + '\'' +
                ", destRocketMQPath='" + destRocketMQPath + '\'' +
                ", pullMaxNums=" + pullMaxNums +
                ", numConsumerPullThreads=" + numConsumerPullThreads +
                '}';
    }
}
