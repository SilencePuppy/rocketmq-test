package org.example.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.example.constant.Constant;

/**
 * @author Li Xiaobing
 * @Classname OneWayProducer
 * @date 2021/9/21 17:31
 */
public class OneWayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(Constant.ONE_WAY_PRODUCER_GROUP);

        defaultMQProducer.setNamesrvAddr(Constant.NAME_SRV_ADDR);

        defaultMQProducer.start();

        for (int i = 0; i < 10; i++) {
            byte[] msg = ("Hi one way "+i).getBytes();
            Message  message= new Message(Constant.ONE_WAY_MSG_TOPIC,null,msg);
            defaultMQProducer.sendOneway(message);
        }
        defaultMQProducer.shutdown();
    }
}
