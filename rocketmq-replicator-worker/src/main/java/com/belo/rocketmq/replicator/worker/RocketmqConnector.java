package com.belo.rocketmq.replicator.worker;

import com.belo.rocketmq.replicator.worker.utils.WorkerConst;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Created by liqiang on 2017/10/11.
 */
public class RocketmqConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketmqConnector.class);

    private WorkerConf workerConf;

    private MQProducer mqProducer;

    private ConsumerPullManager consumerPullManager;

    private CopyOnWriteArraySet<MessageQueue> topicQueueRegistry = new CopyOnWriteArraySet();

    public RocketmqConnector(WorkerConf workerConf, MQProducer mqProducer) {
        this.workerConf = workerConf;
        this.mqProducer = mqProducer;
        this.consumerPullManager = new ConsumerPullManager(this.workerConf, this.mqProducer);

        this.consumerPullManager.start();
    }

    /**
     * 处理Node节点增加MessageQueue的情况
     * @param resourceName
     * @param messageQueueName
     */
    public void addTopicMessageQueue(String resourceName, String messageQueueName) {
        String[] messageQueueInfo = messageQueueName.split(WorkerConst.SEPARATOR);

        MessageQueue messageQueue = new MessageQueue(resourceName, messageQueueInfo[0], Integer.valueOf(messageQueueInfo[1]));

        if(this.topicQueueRegistry.contains(messageQueue)){
            LOGGER.info("Topic:{} and brokername:{} and queue:{} already exist. Ignoring operation",resourceName, messageQueueInfo[0], messageQueueInfo[1]);
            return;
        }

        this.consumerPullManager.addTopicMessageQueue(messageQueue);
        this.topicQueueRegistry.add(messageQueue);
    }

    /**
     * 处理Node节点删除MessageQueue的情况
     * @param resourceName
     * @param messageQueueName
     */
    public void deleteTopicMessageQueue(String resourceName, String messageQueueName) {
        String[] messageQueueInfo = messageQueueName.split(WorkerConst.SEPARATOR);

        MessageQueue messageQueue = new MessageQueue(resourceName, messageQueueInfo[0], Integer.valueOf(messageQueueInfo[1]));

        if (!this.topicQueueRegistry.contains(messageQueue)) {
            LOGGER.info("Topic:{} and messageQueueName:{} don't exist. Ignoring operation", resourceName, messageQueueName);
            return;
        }

        this.consumerPullManager.removeTopicMessageQueue(messageQueue);

        this.topicQueueRegistry.remove(messageQueue);
    }

    public void shutdown() {
        this.consumerPullManager.close();
        this.topicQueueRegistry.clear();
    }
}
