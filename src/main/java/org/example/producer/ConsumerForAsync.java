package org.example.producer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
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
 * @Classname ConsumerForAsync
 * @date 2021/9/21 19:56
 */
public class ConsumerForAsync {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(Constant.CONSUMER_GROUP_FOR_ASYNC_TOPIC);

        defaultMQPushConsumer.setNamesrvAddr(Constant.NAME_SRV_ADDR);

        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        defaultMQPushConsumer.subscribe(Constant.ASYNC_MSG_TOPIC, "*");

        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently(){
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

                for (MessageExt messageExt : list) {
                    String keys = messageExt.getKeys();
                    String topic = messageExt.getTopic();
                    String tags = messageExt.getTags();
                    byte[] body = messageExt.getBody();

                    System.out.println("keys: "+ keys+", topic: "+topic+", tags:"+tags+", body: "+new String(body));
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        defaultMQPushConsumer.start();
    }
}
