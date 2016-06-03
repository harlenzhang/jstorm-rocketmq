package com.tqmall.iserver.rocket;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class RocketClientConfig implements Serializable{

    public static final String TOPIC = "rocket.topic";

    public static final String NAMESERVER = "rocket.nameserver";

    public static final String GROUP = "rocket.group";

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

        if (StringUtils.isBlank((String)conf.get(NAMESERVER)) == false)
            nameServer = (String) conf.get(NAMESERVER);
        else
            nameServer = null;
    }

    public static RocketClientConfig mkInstance(Map conf){

        return new RocketClientConfig(conf);
    }
}
