package com.fhyq.storm;

import java.io.IOException;
import java.text.SimpleDateFormat; 
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;  
import java.util.HashMap;  
import java.util.List;  
import java.util.Map;  
import java.util.Properties;  

import javax.xml.crypto.Data;

import kafka.consumer.ConsumerConfig;  
import kafka.consumer.ConsumerIterator;  
import kafka.consumer.KafkaStream;  
import kafka.javaapi.consumer.ConsumerConnector;  

import org.apache.storm.spout.SpoutOutputCollector;  
import org.apache.storm.task.TopologyContext;  
import org.apache.storm.topology.IRichSpout;  
import org.apache.storm.topology.OutputFieldsDeclarer;  
import org.apache.storm.tuple.Fields;  
import org.apache.storm.tuple.Values;  

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.util.Bytes; 


/*
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan; 
*/

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.RowFilter; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes; 

//import org.apache.logging.log4j.Logger;
//import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

  
public class KafkaStormSpout01 implements IRichSpout {   
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaStormSpout01.class);
    
	private static Configuration config = HBaseConfiguration.create();
	
	private static final long serialVersionUID = 1L;  
    private SpoutOutputCollector collector;  
    private ConsumerConnector consumer;  
    private String topic;  
    private String mineID, faceID, facegasVal, windgasVal, windSpeedVal;
    
    public List<DATA> facegasList = new ArrayList<DATA>();
    public List<DATA> windgasList = new ArrayList<DATA>();
    public List<DATA> windSpeedList = new ArrayList<DATA>();
  
    public KafkaStormSpout01() {
    }  
  
    public KafkaStormSpout01(String topic) {  
        this.topic = topic;  
    } 
  
    public void nextTuple() {  
    }  
  
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {  
        this.collector = collector;  

        logger.info("open kafka client"); 
    }  
  
    public void ack(Object msgId) {  
    }  
  
    public void activate() {  
  
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());  
        Map<String, Integer> topickMap = new HashMap<String, Integer>();  
        topickMap.put(topic, 1);  
  
        logger.info("receive kafka data <topic>:" + topic); 
  
        Map<String, List<KafkaStream<byte[], byte[] >>> streamMap = consumer.createMessageStreams(topickMap);  
          
        KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);  
        ConsumerIterator<byte[],byte[]> it = stream.iterator();  
                
        while (it.hasNext()) {  
            String value = new String(it.next().message());  
            logger.info("storm recived kafka message---->" + value); 
            
            //解析数据 MineID FaceID FaceGasID WindGasID WindSpeedID
            String[] fields = value.split(",");
            
            String mineID = fields[0].replace("\"", "");
            String faceID = fields[1].replace("\"", "");
            String faceGasID = mineID + "01" + fields[2].replace("\"", "");
            String windGasID = mineID + "01" + fields[3].replace("\"", "");
            String windSpeedID = mineID + "01" + fields[4].replace("\"", "");
            
            logger.info("mineID-->" + mineID);
            logger.info("faceID-->" + faceID);
            logger.info("faceGasID-->" + faceGasID);
            logger.info("windGasID-->" + windGasID);
            logger.info("windSpeedID-->" + windSpeedID);
           
        	
        	
        	//根据测点类型发射到不同的Bolt
        	collector.emit(new Values(mineID, faceID, "1", faceGasID));
        	collector.emit(new Values(mineID, faceID, "2", windGasID));
        	collector.emit(new Values(mineID, faceID, "3", windSpeedID));       	
        	
        }  
    }  
   
   private static ConsumerConfig createConsumerConfig() {  
    
        Properties props = new Properties();  
        props.put("zookeeper.connect", "name01:2181,data01:2181,data11:2181");  
        props.put("group.id", "1");     
        props.put("auto.commit.interval.ms", "1000");  
        props.put("zookeeper.session.timeout.ms", "10000");  
        return new ConsumerConfig(props); 
    
    }  
   
    public void close() {  
    }  
  
    public void deactivate() {  
    }  
  
    public void fail(Object msgId) {  
    }  
  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        declarer.declare(new Fields("mineid", "faceid", "type", "cdbh"));
    }

    @Override  
    public Map<String, Object> getComponentConfiguration() {  
    	logger.info("getComponentConfiguration are copied"); 
        topic = "gas02Topic";  
        return null;  
    }
    

    
}  