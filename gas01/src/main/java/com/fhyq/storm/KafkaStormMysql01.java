package com.fhyq.storm;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

import com.google.common.base.Charsets;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStormMysql01 extends BaseBasicBolt {  
	private static final Logger logger = LoggerFactory.getLogger(KafkaStormCount01.class);
    	
    private String mineID = "";
    private String faceID = "";
	private Double value;
    public void execute(Tuple tuple, BasicOutputCollector collector) { 
    	mineID = tuple.getString(0);
    	faceID = tuple.getString(1); 
    	value = tuple.getDouble(2);   
    	
    	logger.info("mineID:" + mineID); 
    	logger.info("faceID:" + faceID);
    	logger.info("value:" + value); 
	 
	 	 
   		final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; 
   		final String DB_URL = "jdbc:mysql://192.168.7.11:3306/mine?useUnicode=true&characterEncoding=UTF-8";
   		
   		final String USER = "root";
   		final String PASS = "hello";
   		try {        		
	        	
   			Connection conn = null;
   			Statement stmt = null;
           
   			Class.forName("com.mysql.jdbc.Driver");
         
   			logger.info("database connecting ...");
   			conn = DriverManager.getConnection(DB_URL, USER, PASS);
   			stmt = conn.createStatement();
   			
   			stmt.execute("set Names utf8");
   			String sql = "INSERT INTO MineFaceGasValue (MineID, FaceID, FaceName, FaceType ,GushAmt, Concentration, DataTime) ";
   			sql += "VALUES('" + mineID + "', '" + faceID + "', '" + new String("²âÊÔ¹¤×÷Ãæ".getBytes(Charsets.UTF_8), "UTF-8") + "', '" + "1" + "', " + value + "," + 0.0 + ", now())";         
		
   			logger.info("sql: " + sql + "\n");   
            
            stmt.execute(sql);
		
         
			stmt.close();
			conn.close();
     	} catch(SQLException e){
     		e.printStackTrace();
     	} catch(Exception e){
     		e.printStackTrace();
     	} 

    
    }  
    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        //declarer.declare(new Fields("cdbh", "unit"));  
    } 
    
}  