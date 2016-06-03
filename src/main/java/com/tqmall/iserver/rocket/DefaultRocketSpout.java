package com.tqmall.iserver.rocket;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class DefaultRocketSpout implements IRichSpout, MessageListenerConcurrently{

    private Map conf;
    private String id;
    private SpoutOutputCollector collector;
    private RocketClientConfig rocketClientConfig;
    private transient DefaultMQPushConsumer consumer;
    private static Logger log = LoggerFactory.getLogger(DefaultRocketSpout.class);


    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.conf = conf;
        this.collector = collector;
        this.id = context.getThisComponentId() + ":" + context.getThisTaskId();

        rocketClientConfig = RocketClientConfig.mkInstance(conf);

        try {
            consumer = ConsumerFactory.mkInstance(rocketClientConfig, this);
        } catch (MQClientException e) {
            log.error("failed to create rocket consumer: {}", e.getErrorMessage());
            throw new RuntimeException("fail to create consumer for component: " + id);
        }

        /**when there was consumer already been started, the consumer will be null*/
        if (consumer == null){
            log.warn("component {} already have consumer fetch data", id);

            new Thread(new Runnable() {

                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e) {
                            break;
                        }
                        log.info("there was one consumer already started, thus the second will do nothing");

                    }
                }
            }).start();
        }
            log.info("Successfully init " + id);

    }

    public void close() {
        if (consumer != null)
            consumer.shutdown();

    }

    public void activate() {
        if (consumer != null)
            consumer.resume();
    }

    public void deactivate() {
        if (consumer != null)
            consumer.suspend();

    }

    public void nextTuple() {

    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {

    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        return null;
    }
}
