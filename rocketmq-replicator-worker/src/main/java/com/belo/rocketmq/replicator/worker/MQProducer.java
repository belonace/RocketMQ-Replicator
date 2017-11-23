package com.belo.rocketmq.replicator.worker;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 发送工作，整个实例只有一个producer
 * Created by liqiang on 2017/10/16.
 */
public class MQProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MQProducer.class);

    private WorkerConf workerConf;

    private DefaultMQProducer producer;

    /**
     * 启动错误，需要抛出异常，进程停止
     * @param controllerConf
     * @throws MQClientException
     */
    public MQProducer(WorkerConf controllerConf) throws MQClientException {
        this.workerConf = controllerConf;

        this.producer = new DefaultMQProducer();

        //设置Producer Group（必须）
        this.producer.setProducerGroup("MQ-R");

        //设置NameServer（必须）
        LOGGER.info("NamesrvAddr:{}", this.workerConf.getDestRocketMQPath());
        this.producer.setNamesrvAddr(this.workerConf.getDestRocketMQPath());

        try {
            this.producer.start();
            LOGGER.info("MQProducer start success!");
        }catch (MQClientException e){
            LOGGER.error("MQProducer start failed!", e);
            throw e;
        }
    }

    /**
     * 因为需要保证消息的顺序性，所以根据取模的算法，固定队列的发送位置
     * 此处发送的消息会发到对等的Topic，即目标必须存在同名的Topic
     * @param sourcemq
     * @param sourceMessage
     * @return
     */
    public SendResult send(MessageQueue sourcemq, MessageExt sourceMessage) throws Exception {
        try {
            //消息实例
            Message msg = new Message(sourceMessage.getTopic(),// topic
                    sourceMessage.getTags(),// tag
                    sourceMessage.getKeys(), //key
                    sourceMessage.getFlag(),//flag
                    sourceMessage.getBody(),// body
                    sourceMessage.isWaitStoreMsgOK()
            );

            //保留用户消息属性
            for(Map.Entry<String, String> entry : sourceMessage.getProperties().entrySet()) {
                if(!MessageConst.STRING_HASH_SET.contains(entry.getKey())) {
                    msg.putUserProperty(entry.getKey(), entry.getValue());
                }
            }

            //发送消息，根据i+1 参数，队列选择的算法为取模队列的长度，此处可以根据自己的需求重写
            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    MessageQueue sourcemq = (MessageQueue) arg;
                    int index = Math.abs(sourcemq.hashCode()) % mqs.size();
                    return mqs.get(index);
                }
            }, sourcemq);

            //打印发送结果，建议在日志中记录msgId，方便排查问题
            //LOGGER.info("send result:{}", sendResult);

            return sendResult;
        } catch (Exception e) {
            //发送失败.需要抛出异常，重试发送
            LOGGER.error("send failed{}", e);
            throw e;
        }
    }

    public void stop() {
        this.producer.shutdown();
    }
}
