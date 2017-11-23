package com.belo.rocketmq.replicator.worker;

import com.belo.rocketmq.replicator.worker.utils.ShutdownableThread;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by liqiang on 2017/10/16.
 */
public class ConsumerPullThread extends ShutdownableThread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerPullThread.class);

    private WorkerConf workerConf;
    private MQProducer mqProducer;
    private String threadid;

    private DefaultMQPullConsumer defaultMQPullConsumer;

    private CopyOnWriteArraySet<MessageQueue> messageQueueSet = new CopyOnWriteArraySet();
    private CopyOnWriteArraySet<MessageQueue> messageQueueDeleteSet = new CopyOnWriteArraySet();
    private CopyOnWriteArraySet<MessageQueue> messageQueueAddSet = new CopyOnWriteArraySet();

    private Lock messageQueueSetLock = new ReentrantLock();

    //定时提交offset任务
    private ScheduledExecutorService sheduler = Executors.newSingleThreadScheduledExecutor();

    public ConsumerPullThread(WorkerConf workerConf, MQProducer mqProducer, String threadid) throws MQClientException {
        super("ConsumerPullThread" + threadid, true);
        this.workerConf = workerConf;
        this.mqProducer = mqProducer;
        this.threadid = threadid;

        defaultMQPullConsumer = new DefaultMQPullConsumer("consumerPullThread-110");
        defaultMQPullConsumer.setInstanceName("ConsumerPullThread-" + threadid);
        defaultMQPullConsumer.setNamesrvAddr(this.workerConf.getSrcRocketMQPath());
        defaultMQPullConsumer.start();

        this.sheduler.scheduleAtFixedRate(new Runnable() {
            public void run() {
                //commitOffset();
            }
        }, 1000, 1000 * 1, TimeUnit.MILLISECONDS);
    }

    /**
     * 提交offset
     */
    public void commitOffset() {
        try {
            Set<MessageQueue> messageQueues = new HashSet<MessageQueue>();
            messageQueues.addAll(messageQueueSet);
            defaultMQPullConsumer.getDefaultMQPullConsumerImpl().getOffsetStore().persistAll(messageQueues);
        } catch (Exception ex) {
            LOGGER.warn("Persist consumer offset error.", ex);
        } finally {
        }
    }

    /**
     * 执行拉取动作
     */
    public void doWork() {
        this.messageQueueSetLock.lock();
        try {
            if (this.messageQueueDeleteSet.size() > 0) {
                this.commitOffset();
                for (MessageQueue messageQueueDelete : this.messageQueueDeleteSet) {
                    if (this.messageQueueSet.contains(messageQueueDelete)) {
                        LOGGER.info("deleteMessageQueue :{} ", messageQueueDelete);
                        this.messageQueueSet.remove(messageQueueDelete);
                    }
                }
                this.messageQueueDeleteSet.clear();
            }

            if (this.messageQueueAddSet.size() > 0) {
                for (MessageQueue messageQueueAdd : this.messageQueueAddSet) {
                    if (!this.messageQueueSet.contains(messageQueueAdd)) {
                        LOGGER.info("addMessageQueue :{} ", messageQueueAdd);
                        this.messageQueueSet.add(messageQueueAdd);
                    }
                }
                this.messageQueueAddSet.clear();
            }
        } finally {
            this.messageQueueSetLock.unlock();
        }

        try {
            Iterator<MessageQueue> iterator = this.messageQueueSet.iterator();
            while (iterator.hasNext()) {
                MessageQueue messageQueue = iterator.next();
                try {
                    //消费位置从最小的offset或者当前的offset开始
                    long offset = this.getMessageQueueOffset(this.defaultMQPullConsumer, messageQueue);

                    PullResult pullResult = this.defaultMQPullConsumer.pull(messageQueue, null, offset, this.workerConf.getPullMaxNums());

                    boolean sendok = true;
                    if (pullResult.getPullStatus() == PullStatus.FOUND) {
                        for (MessageExt messageExt : pullResult.getMsgFoundList()) {
                            try {
                                this.mqProducer.send(messageQueue, messageExt);
                            } catch (Exception e) {
                                LOGGER.error("send message error, send again:{}", e);

                                try {
                                    this.mqProducer.send(messageQueue, messageExt);
                                } catch (Exception e1) {
                                    sendok = false;
                                    LOGGER.error("send message error again, drop it:{}", e);
                                }
                            }
                        }

                        if (sendok) {
                            this.defaultMQPullConsumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                        }
                    }

                } catch (Exception e) {
                    LOGGER.error("pull message error:{}", e);
                }
            }

        } catch (Exception e) {
            LOGGER.error("doWork error:{}", e);
            this.commitOffset();
        } finally {
            //this.messageQueueSetLock.unlock();
            this.commitOffset();
        }
    }

    private long getMessageQueueOffset(DefaultMQPullConsumer consumer, MessageQueue messageQueue) throws MQClientException {
        // 从Broker获取Offset
        long result = 0L;

        long lastOffset = consumer.fetchConsumeOffset(messageQueue, false);
        if (lastOffset >= 0) {
            result = lastOffset;
        } else if (-1 == lastOffset) {
            try {
                result = consumer.getDefaultMQPullConsumerImpl().minOffset(messageQueue);
            } catch (MQClientException e) {
                result = 0L;
            }
        } else {
            result = -1;
        }

        return result;
    }

    @Override
    public void shutdown() {
        LOGGER.info("Close ConsumerPullThread....{}", this.getName());
        this.initiateShutdown();
        this.awaitShutdown();
        LOGGER.info("Close ConsumerPullThread....{}, success", this.getName());

        this.commitOffset();
        this.defaultMQPullConsumer.shutdown();
    }

    public void removeMessageQueues(CopyOnWriteArraySet<MessageQueue> partitionDeleteSet) {
        this.messageQueueSetLock.lock();
        try {
            for (MessageQueue messageQueue : partitionDeleteSet) {
                this.messageQueueDeleteSet.add(messageQueue);
                this.messageQueueAddSet.remove(messageQueue);
            }
        } catch (Exception e) {
            LOGGER.error("removeMessageQueues error:{}", e);
        } finally {
            this.messageQueueSetLock.unlock();
        }
    }

    public void addMessageQueue(MessageQueue messageQueue) {
        this.messageQueueSetLock.lock();
        try {
            Thread.sleep(1000);
            this.messageQueueAddSet.add(messageQueue);
            this.messageQueueDeleteSet.remove(messageQueue);
        } catch (Exception e) {
            LOGGER.error("addMessageQueue error:{}", e);
        } finally {
            this.messageQueueSetLock.unlock();
        }
    }
}
