package org.example.producer.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.example.constant.Constant;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author lixiaobing
 * @Classname TransactionMsgProducer
 * @date 2021/10/12 14:57
 */
public class TransactionMsgProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, InterruptedException {
        TransactionListenerImpl transactionListener = new TransactionListenerImpl();

        ExecutorService executorService = new ThreadPoolExecutor(2, 2, 60, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(100));

        // 使用事务生产者
        TransactionMQProducer producer = new TransactionMQProducer(Constant.TRANSACTION_PRODUCER_GROUP);
        producer.setNamesrvAddr(Constant.NAME_SRV_ADDR);

        producer.setExecutorService(executorService);
        producer.setTransactionListener(transactionListener);

        producer.start();

        for (int i = 0; i < 2; i++) {
            Message message = new Message(Constant.TRANSACTION_MSG_TOPIC, null,
                    ("transaction msg " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message, null);

            System.out.println(transactionSendResult);
            Thread.sleep(1000);
        }

        // 防止生产者过早的结束
        for (int i = 0; i < 1000; i++) {
            Thread.sleep(10000);
        }
        producer.shutdown();
    }
}
