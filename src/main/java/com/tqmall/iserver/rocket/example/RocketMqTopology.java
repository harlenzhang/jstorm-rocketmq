package com.tqmall.iserver.rocket.example;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import com.tqmall.iserver.rocket.DefaultRocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by harlenzhang on 16/6/3.
 */
public class RocketMqTopology {
    private static Logger log = LoggerFactory.getLogger(RocketMqTopology.class);

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        //LoadConf(args[0]);
        LoadConf("/Users/harlenzhang/Documents/projects/jstorm-rocketmq/src/main/resources/rocketspout.yaml");
        TopologyBuilder builder =  new TopologyBuilder();

        builder.setSpout("RocketSpout", new DefaultRocketSpout());
        builder.setBolt("TestBolt", new RocketBolt()).shuffleGrouping("RocketSpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("testLocal", conf, builder.createTopology());
        //StormSubmitter.submitTopology("test", conf, builder.createTopology());

    }



    private static void LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            LoadYaml(arg);
        } else {
            LoadProperty(arg);
        }
    }


    private static void LoadYaml(String confPath) {

        Yaml yaml = new Yaml();

        try {
            InputStream stream = new FileInputStream(confPath);

            conf = (Map) yaml.load(stream);
            if (conf == null || conf.isEmpty() == true) {
                throw new RuntimeException("Failed to read config file");
            }

        } catch (FileNotFoundException e) {
            System.out.println("No such file " + confPath);
            throw new RuntimeException("No config file");
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to read config file");
        }

        return;
    }


    private static Map conf = new HashMap<Object, Object>();

    private static void LoadProperty(String prop) {
        Properties properties = new Properties();

        try {
            InputStream stream = new FileInputStream(prop);
            properties.load(stream);
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + prop);
        } catch (Exception e1) {
            e1.printStackTrace();

            return;
        }

        conf.putAll(properties);
    }
}

