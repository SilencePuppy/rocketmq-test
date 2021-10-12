package org.example.producer.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lixiaobing
 * @Classname TransactionListenerImpl
 * @date 2021/10/12 14:57
 */
public class TransactionListenerImpl implements TransactionListener {

    // 用来将本地事务的状态和事务消息进行关联,可以用redis进行替代
    private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        System.out.println("我是 A 系统，我正在执行我这部分的任务。");
        // 状态应该是查询DB来判断本地执行中BC是否执行完了辅助任务,当状态是2的时候才表明本地事务完成
        Integer status = 0;
        localTrans.put(msg.getTransactionId(), status);

        // 自己将status设置为0，其他的BC会将其各自加一。其实A也应该是从DB取，如果没有设置为0 否则加1
        if (status == 2) {
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (status == -1) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else {
            return LocalTransactionState.UNKNOW;
        }
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        String transactionId = msg.getTransactionId();
        Integer status = localTrans.get(transactionId);
        if (status != null) {
            switch (status) {
                case 0:
                case 1:
                    localTrans.put(transactionId, ++status);
                    return LocalTransactionState.UNKNOW;
                case 2:
                    localTrans.remove(transactionId);
                    return LocalTransactionState.COMMIT_MESSAGE;
                default:
                    localTrans.remove(transactionId);
                    return LocalTransactionState.ROLLBACK_MESSAGE;
            }
        }

        System.out.println("没有本地事务执行状态");
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
