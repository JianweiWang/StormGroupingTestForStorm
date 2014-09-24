package com.wjw.grouping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SentenceSpout extends BaseRichSpout{
	public SpoutOutputCollector _collector = null;
	Long msgId = new Long(0);
	Long startTime = System.currentTimeMillis();
	int timeIntervalFlag = 0;
	long timeInterval[] = {300,480,600,900};
	long sleepTime[] = {1,0,2};
	long finishTime;
	long time = 1;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	String[] sentences = {
			"the cow jumped over the moon", 
			"an apple a day keeps the doctor away",
	       "four score and seven years ago", 
	       "snow white and the seven dwarfs",
	       "i am at two with nature"
	};
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		finishTime = System.currentTimeMillis();
		
//		if(((finishTime - startTime)/1000)%timeInterval[timeIntervalFlag] == 0)
//			{
//				time = sleepTime[timeIntervalFlag];
//				timeIntervalFlag = (timeIntervalFlag+1)%timeInterval.length;
//			}
		if(msgId % 500000 == 0) {
			time = sleepTime [(int) ((msgId / 500000) % (sleepTime.length))];
			//System.out.println("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA sleep :" + time);
		}
		Utils.sleep(time);
		Random rand = new Random(System.currentTimeMillis());
		String sentence = sentences[rand.nextInt(sentences.length)];
		//_collector.emit(new Values(sentence));
		_collector.emit(new Values(sentence),msgId);
		msgId++;
		
		//_collector.emitDirect(34, "sentence", new Values(sentence));
		//count++;
//		if(msgId == 100)
//			return;
	}

	@Override
	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		System.out.println("messageId: " + msgId);
		
	}

	@Override
	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("sentence"));
		//declarer.declareStream("sentence", true, new Fields("sentence"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
