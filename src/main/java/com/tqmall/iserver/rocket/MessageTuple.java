package com.tqmall.iserver.rocket;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.io.Serializable;
import java.util.List;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class MessageTuple implements Serializable{

    private final List<MessageExt> msglist;
    private final MessageQueue messageQueue;

    public MessageTuple(List<MessageExt> msglist, MessageQueue queue){
        this.msglist = msglist;
        this.messageQueue = queue;
    }


}
