package com.fhyq.storm;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;  
import java.util.List;
import java.util.Map;  
import java.util.concurrent.atomic.AtomicInteger;  
  
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


/*
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
*/

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan; 

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList; 
import org.apache.hadoop.hbase.filter.RowFilter; 
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes; 

import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStormHBase01 extends BaseBasicBolt {  
	private static final Logger logger = LoggerFactory.getLogger(KafkaStormHBase01.class);

    private BasicOutputCollector collector;  
	private static Configuration config = HBaseConfiguration.create();
	private HTable table;
    
    private final int countPeriod = 7 * 24;	//计算周期 1周
    	
    private String cdbh = "";
    private String type = "";
	Map<String,Double> result = new HashMap<String,Double>();
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		logger.info("*********prepare********"); 		

	}
	
    public void execute(Tuple tuple, BasicOutputCollector collector) { 
    	this.collector = collector;
    	
    	String mineID = tuple.getString(0).trim();  
    	String faceID = tuple.getString(1).trim();       	
    	String type = tuple.getString(2).trim();       	
    	String cdbh = tuple.getString(3).trim();       	     	
      	 
        logger.info("mineID:" + mineID); 
        logger.info("faceID:" + faceID);
        logger.info("type:" + type);
        logger.info("cdbh:" + cdbh);    	 
        
        if (type.equals("1")) {
        	facegasDeal(mineID, faceID, cdbh);
        } else if (type.equals("2")) {
        	windgasDeal(mineID, faceID, cdbh);
        } else if (type.equals("3")) {
        	windSpeedDeal(mineID, faceID, cdbh);
        }
  
    }      

    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
        declarer.declare(new Fields("mineid","faceid","type", "value"));  
    } 

    public void facegasDeal(String mineID, String faceID, String cdbh) {
        String startRow;	
        String stopRow;	
        
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("SSSJ"));

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		
		//提取工作面测点（cdbhFaceGas）数据到数据对象数组
		Calendar calendar = Calendar.getInstance();
		stopRow = cdbh + sdf.format(calendar.getTime());
		
        calendar.set(Calendar.HOUR_OF_DAY, calendar.get(Calendar.HOUR_OF_DAY) - countPeriod);
        startRow = cdbh + sdf.format(calendar.getTime());		
        
        RowFilter filter1 = new RowFilter(CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(startRow)));
        RowFilter filter2 = new RowFilter(CompareOp.LESS, new BinaryComparator(Bytes.toBytes(stopRow)));

        List<Filter> filters = new ArrayList<Filter>();  
        filters.add(filter1);  
        filters.add(filter2);  
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);        

        scan.setFilter(fl);
                   
        try {
        	table = new HTable(config, "GAS");
        	logger.info("open table success!");
            ResultScanner scanner = table.getScanner(scan);
            logger.info("start scanning------->" ); 
        	for (Result r : scanner) {
        		logger.info("scanning..."); 
				for(KeyValue kv:r.raw())  
                {  
                    logger.info("getRow message------->" + Bytes.toString(kv.getRow())); 
                    logger.info("getFamily message------->" + Bytes.toString(kv.getFamily()));
                    logger.info("getQualifier message------->" + Bytes.toString(kv.getQualifier()));
                    logger.info("getValue message------->" + Bytes.toString(kv.getValue()));
                
                     
                } 
				
				//计算value = (ti+1 - ti)*(Ci+1 - Ci)*V
				//collector.emit(new Values("1", value));
				collector.emit(new Values(mineID, faceID, "1", 100.0)); 
            }  

        } catch (Exception e) {  
        	// TODO: handle exception  
        	e.printStackTrace();  
        } finally{
        	try {
        		if (table != null) table.close();
        	} catch (Exception e) { 
            	e.printStackTrace();  
            }
        }  
    }
    
    public void windgasDeal(String mineID, String faceID, String cdbh) {  
        String cdbhWindGas = "";	
        String startRow;	
        String stopRow;	
        
        //计算value = (ti+1 - ti)*(Ci+1 - Ci)*V
		//collector.emit(new Values("2",value)); 
        collector.emit(new Values(mineID, faceID, "2", 200.0));
    }
    
    public void windSpeedDeal(String mineID, String faceID, String cdbh) { 
        String cdbhWindSpeed = "";		
        String startRow;	
        String stopRow;	
        
        //计算value = (ti+1 - ti)*(Ci+1 - Ci)*V
		//collector.emit(new Values("3",value));
        collector.emit(new Values(mineID, faceID, "3", 300.0));
    }
}  