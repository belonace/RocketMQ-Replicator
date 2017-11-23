package com.belo.rocketmq.replicator.controller.core;

import com.google.common.collect.ImmutableSet;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * 实例的当前Topic队列，实例分配的队列基本都是通过简单的循环来分配的，有没有更好的策略？
 * Created by liqiang on 2017/9/28.
 */
public class InstanceTopicQueueHolder {

    private final String instanceName;

    private final Set<MessageQueue> topicQueueSet = new HashSet<MessageQueue>();

    public InstanceTopicQueueHolder(String instance) {
        this.instanceName = instance;
    }

    public String getInstanceName() {
        return this.instanceName;
    }

    public Set<MessageQueue> getServingTopicQueueSet() {
        return ImmutableSet.copyOf(this.topicQueueSet);
    }

    public int getNumServingTopicQueues() {
        return this.topicQueueSet.size();
    }

    public void addTopicQueue(MessageQueue topicQueue) {
        this.topicQueueSet.add(topicQueue);
    }

    public void removeTopicQueue(MessageQueue topicQueue) {
        this.topicQueueSet.remove(topicQueue);
    }

    /**
     * 比较排序每个实例拥有的Queue的大小，倒序排列
     * @return
     */
    public static Comparator<InstanceTopicQueueHolder> getComparator() {
        return new Comparator<InstanceTopicQueueHolder>() {
            public int compare(InstanceTopicQueueHolder o1, InstanceTopicQueueHolder o2) {
                int size1 = (o1 == null) ? -1 : o1.getNumServingTopicQueues();
                int size2 = (o2 == null) ? -1 : o2.getNumServingTopicQueues();
                if (size1 != size2) {
                    return size1 - size2;
                } else {
                    return o1.getInstanceName().compareTo(o2.getInstanceName());
                }

            }
        };
    }

    public void addTopicQueues(Collection<MessageQueue> topicQueueInfos) {
        this.topicQueueSet.addAll(topicQueueInfos);
    }

    @Override
    public int hashCode() {
        return this.instanceName.hashCode();
    }

}
