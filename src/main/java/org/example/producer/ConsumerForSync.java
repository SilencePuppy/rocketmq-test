package org.example.producer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.example.constant.Constant;

import java.util.List;

/**
 * @author Li Xiaobing
 * @Classname MyConsumer
 * @date 2021/9/21 19:41
 */
public class ConsumerForSync {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(Constant.CONSUMER_GROUP_FOR_SYNC_TOPIC);

        defaultMQPushConsumer.setNamesrvAddr(Constant.NAME_SRV_ADDR);

        defaultMQPushConsumer.subscribe(Constant.SYNC_MSG_TOPIC, MessageSelector.bySql("(TAGS is not null and TAGS " +
                "in ('TagA','TagB') and (name is not null and age between 0 and 3))"));

        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        // 设置为广播模式
        // defaultMQPushConsumer.setMessageModel(MessageModel.BROADCASTING);
        // defaultMQPushConsumer.setMessageModel(MessageModel.CLUSTERING); 默认就是集群模式

        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
                try {
                    for (MessageExt messageExt : list) {
                        String keys = messageExt.getKeys();
                        byte[] body = messageExt.getBody();
                        String topic = messageExt.getTopic();
                        String tags = messageExt.getTags();

                        System.out.println("keys: " + keys + ", topic: " + topic + ", tags:" + tags + ", body: " + new String(body));
                    }
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        defaultMQPushConsumer.start();
    }
}
