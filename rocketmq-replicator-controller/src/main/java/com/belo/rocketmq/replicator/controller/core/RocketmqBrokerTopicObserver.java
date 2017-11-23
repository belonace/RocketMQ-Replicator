package com.belo.rocketmq.replicator.controller.core;

import com.belo.rocketmq.replicator.controller.reporter.HelixRocketMQReplicatorMetricsReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableSet;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by liqiang on 2017/9/28.
 */
public class RocketmqBrokerTopicObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketmqBrokerTopicObserver.class);

    private final ScheduledExecutorService observerService = Executors.newScheduledThreadPool(1);
    private final String rocketmqClusterName;
    private final Map<String, TopicPublishInfo> topicQueueInfoMap = new ConcurrentHashMap<String, TopicPublishInfo>();
    private final Timer refreshLatency = new Timer();
    private final Counter rocketMQTopicsCounter = new Counter();
    private final static String METRIC_TEMPLATE = "RocketmqBrokerTopicObserver.%s.%s";
    private final String nameSrvAddr;

    public RocketmqBrokerTopicObserver(String brokerClusterName, String nameServerString) {

        LOGGER.info("Trying to init RocketMQBrokerTopicObserver {} with NameServer: {}", brokerClusterName, nameServerString);

        this.rocketmqClusterName = brokerClusterName;

        registerMetric();

        this.nameSrvAddr = nameServerString;

        //因为rocketmq的机制，所以使用自动处理的方式
        //暂定一分钟一次主动侦测broker的topic列表变化
        observerService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                try {
                    handleChildChange();
                } catch (Exception e) {
                    LOGGER.error("rocketmq handleChildChange error:", e);
                }
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    private void registerMetric() {
        try {
            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    String.format(METRIC_TEMPLATE, this.rocketmqClusterName, "refreshLatency"), this.refreshLatency);

            HelixRocketMQReplicatorMetricsReporter.get().registerMetric(
                    String.format(METRIC_TEMPLATE, this.rocketmqClusterName, "rocketmqTopicsCounter"), this.rocketMQTopicsCounter);

        } catch (Exception e) {
            LOGGER.error("Failed to register metrics to HelixRocketMQMetricsReporter " + e);
        }
    }

    private void handleChildChange() throws Exception {
        //分为两步
        //取得当前所有的Topic的列表，然后列表中移除内存中的列表，那就是新增的列表
        //然后当前所有的Topic的列表中如果没有内存中的topic，则移除改topic

        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt(this.rocketmqClusterName);
        mqAdminExt.setInstanceName(this.rocketmqClusterName);
        mqAdminExt.setNamesrvAddr(nameSrvAddr);

        try {
            mqAdminExt.start();

            TopicList topicList = null;

            try {
                topicList = mqAdminExt.fetchAllTopicList();
            } catch (Exception e) {
                LOGGER.error("fetchAllTopicList error, nameSrvAddr:{}", this.nameSrvAddr, e);
            }

            if (topicList != null) {
                List<String> currentChilds = new ArrayList<String>(topicList.getTopicList());

                Set<String> currentServingTopics = getAllTopics();

                //其实topic的变化基本不会发生，特别是线上，频率不是很高
                for (String existedTopic : currentServingTopics) {
                    if (!currentChilds.contains(existedTopic)) {
                        this.topicQueueInfoMap.remove(existedTopic);
                    }
                }

                for (String topic : currentChilds) {
                    if(topic.startsWith("%DLQ%") || topic.startsWith("%RETRY%")){
                        continue;
                    }

                    try {
                        TopicPublishInfo topicPublishInfo = this.getTopicPublishInfo(mqAdminExt, topic);
                        this.topicQueueInfoMap.put(topic, topicPublishInfo);
                    } catch (Exception e) {
                        LOGGER.error("Failed to get topic Queue info for {} from rocketmq : {}", topic, this.nameSrvAddr, e);
                    }
                }

                LOGGER.info("Now serving {} topics in RocketMQ cluster: {}", this.topicQueueInfoMap.size(), this.rocketmqClusterName);

                this.rocketMQTopicsCounter.inc(this.topicQueueInfoMap.size());
            }
        } catch (Exception e) {
            LOGGER.error("handleChildChange error", e);
        } finally {
            mqAdminExt.shutdown();
        }
    }

    private TopicPublishInfo getTopicPublishInfo(DefaultMQAdminExt mqAdminExt, String topic) throws RemotingException, MQClientException, InterruptedException {
        //得到每个topic的路由信息，然后算出来所有的queue大小
        TopicRouteData topicRouteData = mqAdminExt.examineTopicRouteInfo(topic);

        if (topicRouteData != null) {
            TopicPublishInfo topicPublishInfo = MQClientInstance.topicRouteData2TopicPublishInfo(topic, topicRouteData);

            return topicPublishInfo;
        }

        throw new MQClientException("Unknow why, Can not find Message Queue for this topic, " + topic, null);
    }

    public TopicPublishInfo getTopicPartition(String topic) {
        return this.topicQueueInfoMap.get(topic);
    }

    public Set<String> getAllTopics() {
        return ImmutableSet.copyOf(this.topicQueueInfoMap.keySet());
    }

    public long getNumTopics() {
        return this.topicQueueInfoMap.size();
    }

    public void stop() {
        observerService.shutdown();
    }

}
