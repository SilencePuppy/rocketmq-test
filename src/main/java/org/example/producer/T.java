package org.example.producer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * @author lixiaobing
 * @Classname T
 * @date 2021/9/22 13:48
 */
public class T {
    public static void main(String[] args) throws MQClientException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("group_name");
        defaultMQProducer.setDefaultTopicQueueNums(8);
        defaultMQProducer.setSendMsgTimeout(3000);
        defaultMQProducer.setCompressMsgBodyOverHowmuch(1024*4);
        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(3);
        defaultMQProducer.setRetryAnotherBrokerWhenNotStoreOK(false);
        defaultMQProducer.setMaxMessageSize(1024*1024*4);//4M
        defaultMQProducer.fetchPublishMessageQueues("topic");

        DefaultMQPushConsumer defaultMQPushConsumer=new DefaultMQPushConsumer("group_name");
        defaultMQPushConsumer.setMessageModel(MessageModel.CLUSTERING);//MessageModel.BROADCASTING
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        defaultMQPushConsumer.setConsumeThreadMin(20);
        defaultMQPushConsumer.setConsumeThreadMax(20);
        defaultMQPushConsumer.setPullInterval(0);
        defaultMQPushConsumer.setPullBatchSize(32);
        defaultMQPushConsumer.setMaxReconsumeTimes(-1);
        defaultMQPushConsumer.setConsumeTimeout(15);
        defaultMQPushConsumer.fetchSubscribeMessageQueues("topicName");

    }
}
