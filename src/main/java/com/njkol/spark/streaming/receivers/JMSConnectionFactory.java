package com.njkol.spark.streaming.receivers;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * A static factory for getting JMS Connection
 * 
 * @author Nilanjan Sarkar
 *
 */
public class JMSConnectionFactory {

	private static final Map<String, Connection> CACHE = new HashMap<String, Connection>();

	public static Connection getConnection(String brokerURL, String userId, String password) throws JMSException {

		if (CACHE.containsKey(brokerURL)) {
			return CACHE.get(brokerURL);
		} else {
			ConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
			Connection conn = factory.createConnection(userId, password);
			CACHE.put(brokerURL, conn);
			return conn;
		}
	}
}
