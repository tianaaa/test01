package com.fhyq.storm;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.HashMap;  
import java.util.Map;  
import java.util.concurrent.atomic.AtomicInteger;  
  
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;    

import com.fhyq.storm.KafkaStormSpout01;
import com.fhyq.storm.KafkaStormHBase01;
import com.fhyq.storm.KafkaStormCount01;
import com.fhyq.storm.KafkaStormMysql01;

public class KafkaStormTopology01 {  
	//private static final Logger logger = LogManager.getLogger("mylog");
	
	private static final String DATA_SPOUT = "DATA_SPOUT";
	private static final String HBASE_BOLT = "HBASE_BOLT";	
	private static final String COUNT_BOLT = "COUNT_BOLT";
	private static final String MYSQL_BOLT = "MYSQL_BOLT";

    public static void main(String[] args) {  
    	//logger.info("main function starting...");
    	
    	Config conf = new Config();
    	conf.setDebug(false);
    	
    	KafkaStormSpout01 spout = new KafkaStormSpout01();
    	KafkaStormHBase01 bolt = new KafkaStormHBase01();
    	KafkaStormCount01 bolt1 = new KafkaStormCount01();
    	KafkaStormMysql01 mysql = new KafkaStormMysql01();

        
        
        TopologyBuilder builder = new TopologyBuilder(); 
        
        builder.setSpout(DATA_SPOUT, spout, 1);
        builder.setBolt(HBASE_BOLT, bolt, 1).fieldsGrouping(DATA_SPOUT, new Fields("type"));  
        //builder.setBolt(HBASE_BOLT, bolt, 3).shuffleGrouping(DATA_SPOUT);
        builder.setBolt(COUNT_BOLT, bolt1, 3).shuffleGrouping(HBASE_BOLT);  
        builder.setBolt(MYSQL_BOLT, mysql, 1).shuffleGrouping(COUNT_BOLT); 
        //builder.setBolt(MYSQL_BOLT, mysql, 1).fieldsGrouping(COUNT_BOLT, new Fields("cdbh")); 
        
        
        /*
        builder.setSpout("spout", new KafkaStormSpout01(""), 1);
        builder.setBolt("bolt1", new Bolt1(), 1).shuffleGrouping("spout");  
        builder.setBolt("bolt2", new Bolt2(), 1).fieldsGrouping("bolt1",new Fields("cdbh"));   
        builder.setBolt("mysql", new KafkaStormMysql01(), 1).fieldsGrouping("bolt2",new Fields("cdbh"));
       */
      
        
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            try{
            	StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
            	//StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
           }
           catch(Exception e){
        	   e.printStackTrace();
           }
        }
          else {
            //conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka-storm", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("kafka-storm");
            cluster.shutdown();
         }
         
    } 

    
}  