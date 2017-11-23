package com.belo.rocketmq.replicator.worker;

import com.belo.rocketmq.replicator.worker.utils.ShutdownableThread;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 管理和维护ConsumerPullThread
 * Created by liqiang on 2017/10/16.
 */
public class ConsumerPullManager extends ShutdownableThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPullManager.class);

    private WorkerConf workerConf;

    private MQProducer mqProducer;

    private Lock updateMapLock = new ReentrantLock();
    private CopyOnWriteArraySet<MessageQueue> messageQueueAddSet = new CopyOnWriteArraySet();
    private CopyOnWriteArraySet<MessageQueue> messageQueueDeleteSet = new CopyOnWriteArraySet();

    private Map<String, ConsumerPullThread> consumerPullThreadMap = new ConcurrentHashMap<String, ConsumerPullThread>(50);

    //private Lock consumerPullThreadMapLock = new ReentrantLock();

    public ConsumerPullManager(WorkerConf workerConf, MQProducer mqProducer) {
        super("ConsumerPullManager-Thread", true);
        this.workerConf = workerConf;
        this.mqProducer = mqProducer;
    }

    /**
     * 增加messageQueue的处理
     *
     * @param messageQueue
     */
    public void addTopicMessageQueue(MessageQueue messageQueue) {
        this.updateMapLock.lock();
        try {
            this.messageQueueAddSet.add(messageQueue);
            this.messageQueueDeleteSet.remove(messageQueue);
        } finally {
            this.updateMapLock.unlock();
        }
    }

    public void removeTopicMessageQueue(MessageQueue messageQueue) {
        this.updateMapLock.lock();
        try {
            this.messageQueueDeleteSet.add(messageQueue);
            this.messageQueueAddSet.remove(messageQueue);
        } finally {
            this.updateMapLock.unlock();
        }
    }

    private void doUpdate() {
        this.updateMapLock.lock();
        try {
            //先处理删除的，在处理增加的MessageQueue

            if (this.messageQueueDeleteSet.size() > 0) {
                this.removeConsumerPullThreadForMessageQueue(this.messageQueueDeleteSet);
                this.messageQueueDeleteSet.clear();
            }

            if (this.messageQueueAddSet.size() > 0) {
                this.addConsumerPullThreadForMessageQueue(this.messageQueueAddSet);
                this.messageQueueAddSet.clear();
            }
        } catch (Exception e) {
            LOGGER.error("ConsumerPullManager doWork error:{}", e);
        } finally {
            this.updateMapLock.unlock();
        }
    }

    public void doWork() {
        this.doUpdate();
    }

    private void addConsumerPullThreadForMessageQueue(CopyOnWriteArraySet<MessageQueue> messageQueueAddSet) {
        try {
            for (MessageQueue messageQueue : messageQueueAddSet) {
                //String messageQueueMode = Math.abs(messageQueue.hashCode()) % this.workerConf.getNumConsumerPullThreads() + "";
                String messageQueueMode = "demo";

                ConsumerPullThread consumerPullThread = null;
                if (this.consumerPullThreadMap.get(messageQueueMode) != null) {
                    consumerPullThread = this.consumerPullThreadMap.get(messageQueueMode);
                } else {
                    consumerPullThread = new ConsumerPullThread(this.workerConf, this.mqProducer, messageQueueMode);
                    this.consumerPullThreadMap.put(messageQueueMode, consumerPullThread);
                    consumerPullThread.start();
                }

                consumerPullThread.addMessageQueue(messageQueue);
            }
        } catch (Exception e) {
            LOGGER.error("addConsumerPullThreadForMessageQueue error : {}", e);
        } finally {
        }
    }

    private void removeConsumerPullThreadForMessageQueue(CopyOnWriteArraySet<MessageQueue> messageQueueDeleteSet) {
        try {
            for (Map.Entry<String, ConsumerPullThread> entry : this.consumerPullThreadMap.entrySet()) {
                //entry.getValue().commitOffset();
                entry.getValue().removeMessageQueues(messageQueueDeleteSet);
            }
        } catch (Exception e) {
            LOGGER.error("removeConsumerPullThreadForMessageQueue error : {}", e);
        } finally {
        }
    }

    public void close() {

        //首先停止Manager
        LOGGER.info("Close ConsumerPullManager....");
        this.shutdown();
        LOGGER.info("Close ConsumerPullManager....success!");

        //其次清空Map
        this.messageQueueAddSet.clear();
        this.messageQueueDeleteSet.clear();

        //最后关闭Thread
        LOGGER.info("Close AllConsumerPullThread....");
        this.closeAllConsumerPullThread();
        LOGGER.info("Close AllConsumerPullThread....success");
    }

    private void closeAllConsumerPullThread() {
        try {
            for (Map.Entry<String, ConsumerPullThread> entry : this.consumerPullThreadMap.entrySet()) {
                entry.getValue().shutdown();
            }
        } catch (Exception e) {
            LOGGER.error("ConsumerPullManager closeAllConsumerPullThread error:{}", e);
        } finally {
        }
    }
}
