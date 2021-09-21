package org.example.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.example.constant.Constant;

import java.util.concurrent.TimeUnit;

/**
 * @author Li Xiaobing
 * @Classname AsyncProducer
 * @date 2021/9/21 16:48
 */
public class AsyncProducer {
    public static void main(String[] args) throws RemotingException, MQClientException, InterruptedException {
        DefaultMQProducer defaultMQProducer= new DefaultMQProducer(Constant.ASYNC_PRODUCER_GROUP);
        defaultMQProducer.setNamesrvAddr(Constant.NAME_SRV_ADDR);

        defaultMQProducer.setRetryTimesWhenSendAsyncFailed(3);

        defaultMQProducer.setSendMsgTimeout(5000);

        defaultMQProducer.setDefaultTopicQueueNums(2);

        defaultMQProducer.start();

        for (int i = 0; i < 10; i++) {
            byte[] msg = ("Bye async "+i).getBytes();
            Message message = new Message(Constant.ASYNC_MSG_TOPIC,null,msg);
            message.setKeys("async key "+i);
            defaultMQProducer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println(throwable.getMessage());
                }
            });
        }

        TimeUnit.SECONDS.sleep(3);
        defaultMQProducer.shutdown();
    }
}
