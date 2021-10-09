package org.example.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.example.constant.Constant;

/**
 * @author Li Xiaobing
 * @Classname SyncProducer
 * @date 2021/9/20 21:42
 */
public class SyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(Constant.SYNC_PRODUCER_GROUP);
        defaultMQProducer.setNamesrvAddr(Constant.NAME_SRV_ADDR);
        // 默认2次
        defaultMQProducer.setRetryTimesWhenSendFailed(3);
        // 默认3s
        defaultMQProducer.setSendMsgTimeout(5000);

        defaultMQProducer.start();

        for (int i = 0; i < 10; i++) {
            byte[] msgBody = ("Hi," + i).getBytes();
            Message message = new Message(Constant.SYNC_MSG_TOPIC,null,msgBody);
            // 同步阻塞等待
            SendResult sendResult = defaultMQProducer.send(message);
            System.out.println(sendResult);
        }
        defaultMQProducer.shutdown();
    }
}
