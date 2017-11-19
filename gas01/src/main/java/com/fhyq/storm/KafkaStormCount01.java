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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStormCount01 extends BaseBasicBolt {  
	private static final Logger logger = LoggerFactory.getLogger(KafkaStormCount01.class);
    	
    private String oldcdbh = "";
    private String type = "";
	Map<String,Double> unit = new HashMap<String,Double>();
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	String mineID = tuple.getString(0).trim();
    	String faceID = tuple.getString(1).trim();
    	String type = tuple.getString(2).trim();
    	Double value = tuple.getDouble(3);
    	
    	logger.info("mineID:" + mineID); 
    	logger.info("faceID:" + faceID);
    	logger.info("type:" + type);
    	logger.info("value:" + value); 
    	
    	if (!oldcdbh.trim().equals(mineID + faceID)) { 
    		
    		oldcdbh = mineID + faceID;
    		
    		if (unit.size() > 0) {
    			
    			//for (String key : unit.keySet()) {
       		 	//}
    			
    			Double sum = unit.get("1") + unit.get("2") - unit.get("3");
    			collector.emit(new Values(mineID, faceID, sum));
    			logger.info(oldcdbh + "-->sum:" + sum);
    			
       		 	logger.info("unit.clear...");
        		unit.clear();
   		 	}
    		unit.put("1", 0.0);
    		unit.put("2", 0.0);
    		unit.put("3", 0.0);
    	}

    	logger.info("正在计算类型为[" + type + "]的值");    	
		Double count = unit.get(type) + value;
		logger.info("类型为[" + type + "]的计算结果为：" + count);
        unit.put(type, count);    
       
    }  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        declarer.declare(new Fields("mineid", "faceid", "sum"));  
    } 

    
}  