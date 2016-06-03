package com.tqmall.iserver.rocket.example;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class RocketBolt implements IRichBolt{
    private Logger log = LoggerFactory.getLogger(RocketBolt.class);

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

    }

    public void execute(Tuple input) {
        String body = input.getString(0);
        log.info("harlen message received:{}", body);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
