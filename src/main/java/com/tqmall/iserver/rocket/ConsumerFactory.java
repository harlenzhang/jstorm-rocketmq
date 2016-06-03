package com.tqmall.iserver.rocket;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class ConsumerFactory {

    private static Logger log = LoggerFactory.getLogger(ConsumerFactory.class);

    public static Map<String, DefaultMQPushConsumer> consumers = new HashMap<String, DefaultMQPushConsumer>();

    public static synchronized DefaultMQPushConsumer mkInstance(RocketClientConfig config, MessageListenerConcurrently listener) throws MQClientException {

        String topic = config.getTopic();
        String consumerGroup = config.getGroup();
        String key = topic + "@" + consumerGroup;

        DefaultMQPushConsumer consumer = consumers.get(key);
        if (consumer != null ){
            log.info("Consumer: {} already exists, don't need recreate it", key);
            return null;
        }

        log.info("start to init consumer client, configuration: ", JSON.toJSONString(config));


        String nameServer = config.getNameServer();
        consumer.setNamesrvAddr(nameServer);

        String tags = config.getTags();
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.subscribe(topic, tags);
            consumer.registerMessageListener(listener);

        consumer.start();

        consumers.put(key, consumer);
        log.info("Successfully create {} consumer", key);

        return consumer;

    }
}
