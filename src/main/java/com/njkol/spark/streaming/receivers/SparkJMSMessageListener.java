package com.njkol.spark.streaming.receivers;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * 
 * @author Nilanjan Sarkar
 *
 */
public class SparkJMSMessageListener implements MessageListener {

	private static final Logger log = LogManager.getRootLogger();
	private Receiver<String> receiver;
	private List<String> buffer = new ArrayList<String>();
	private int blockOffset = 0;
	private Map<Integer, Message> blockMap = new WeakHashMap<Integer, Message>();

	public SparkJMSMessageListener(Receiver<String> receiver) {
		this.receiver = receiver;
	}

	/**
	 * Situation 1 : Before storing in Spark's memory, the receiver crashes
	 * Expectation : The message should not be dequeued and should be re-sent
	 * 
	 * Situation 2 : While storing in Spark's memory, the receiver crashes
	 * Expectation : The message should not be dequeued and should be re-sent
	 */
	public void onMessage(Message message) {
		try {
			log.info("New Message received : " + message);
			if (message instanceof TextMessage) {

				String input = ((TextMessage) message).getText();

				if (blockOffset == 4) {
					receiver.store(buffer.iterator());
					log.info("Data stored reliably in spark memory");
					// Acknowledge the buffered message
					for (Map.Entry<Integer, Message> entry : blockMap.entrySet()) {
						Message msg = entry.getValue();
						msg.acknowledge();
						Integer key = entry.getKey();
						blockMap.remove(key);
					}
					// reset offset
					blockOffset = 0;
				} else {
					// Buffer the data
					buffer.add(input);
					blockOffset++;
					blockMap.put(blockOffset, message);
				}
			}
		} catch (Exception exp) {
			log.error("Caught exception, while storing message in Spark Memmory", exp);
			receiver.reportError("Caught exception, while storing message in Spark Memmory", exp);
		}
	}
}
