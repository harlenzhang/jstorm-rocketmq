package com.tqmall.iserver.rocket;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class RocketClientConfig implements Serializable{

    public static final String TOPIC = "rocketmq.topic";

    public static final String NAMESERVER = "rocketmq.nameserver";

    public static final String GROUP = "rocketmq.group";

    public static final String TAGS = "message.tag";

    private final String nameServer;

    private final String topic;

    private final String group;

    private final String tags;

    public String getNameServer() {
        return nameServer;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroup() {
        return group;
    }

    public String getTags() {
        return tags;
    }

    private RocketClientConfig(Map conf){
        topic = (String) conf.get(TOPIC);
        group = (String) conf.get(GROUP);
        tags = (String) conf.get(TAGS);
        nameServer = (String) conf.get(NAMESERVER);
    }

    public String toString(){
        return ToStringBuilder.reflectionToString(this);
    }

    public static RocketClientConfig mkInstance(Map conf){
        return new RocketClientConfig(conf);
    }
}
