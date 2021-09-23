package org.example.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.example.constant.Constant;

import java.util.List;
import java.util.Random;

/**
 * @author lixiaobing
 * @Classname SeqProducer
 * @date 2021/9/23 9:57
 */
public class SeqProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(Constant.SEQ_PRODUCER_GROUP);
        defaultMQProducer.setNamesrvAddr(Constant.NAME_SRV_ADDR);
        // 设置全局顺序时，需要将默认队列数量修改为1
//        defaultMQProducer.setDefaultTopicQueueNums(1);
        defaultMQProducer.setDefaultTopicQueueNums(2);

        defaultMQProducer.start();

        Random random= new Random(System.currentTimeMillis());
        for (int i = 0; i < 10; i++) {
            Integer orderId = random.nextInt(100);
            byte[] msg = ("Hi seq msg "+i).getBytes();
            Message message = new Message(Constant.SEQ_MSG_TOPIC,null,msg);
            defaultMQProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object arg) {
                    Integer orderId = (Integer) arg;
                    int index = orderId % 2;
                    if (index == 0) {
                        // 偶数
                        return list.get(0);
                    } else {
                        // 奇数
                        return list.get(1);
                    }
                }
            },orderId);
        }

        defaultMQProducer.shutdown();
    }
}
