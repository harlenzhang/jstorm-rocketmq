import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class ConsumerTest {
    public static void main(String[] args) {
        DefaultMQPushConsumer consumer=new DefaultMQPushConsumer("broker-a");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        try {

            // 订阅PushTopic下Tag为push的消息,都订阅消息
            consumer.subscribe("PushTopic", "*");

            // 程序第一次启动从消息队列头获取数据
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            //可以修改每次消费消息的数量，默认设置是每次消费一条
            // consumer.setConsumeMessageBatchMaxSize(10);

            //注册消费的监听
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                //在此监听中消费信息，并返回消费的状态信息
                public ConsumeConcurrentlyStatus consumeMessage(
                        List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {

                    // msgs中只收集同一个topic，同一个tag，并且key相同的message
                    // 会把不同的消息分别放置到不同的队列中
                    for(Message msg:msgs){

                        System.out.println(new String(msg.getBody()));
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();
            Thread.sleep(5000);
            //5秒后挂载消费端消费
            consumer.suspend();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
