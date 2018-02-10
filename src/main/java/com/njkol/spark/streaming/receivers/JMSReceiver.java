package com.njkol.spark.streaming.receivers;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * @author Nilanjan Sarkar
 *         <p>
 * 
 *         A Reliable receiver implementation for listening to a JMS queue for
 *         streaming data The message received from JMS broker is acknowledged
 *         only after it is stored in Spark's Memory
 */
public class JMSReceiver extends Receiver<String> {

	private static final long serialVersionUID = 1L;
	private static final Logger log = LogManager.getRootLogger();

	private String brokerURL;
	private String queueName;
	private String userId;
	private String password;

	private Connection connection;
	private Session session;
	private MessageConsumer consumer;

	public JMSReceiver(String brokerURL, String queueName, String userId, String password, StorageLevel storageLevel) {

		super(storageLevel);
		this.brokerURL = brokerURL;
		this.queueName = queueName;
		this.userId = userId;
		this.password = password;
		log.info(" New JMSReceiver created at : " + System.currentTimeMillis());
	}

	/**
	 * On start of the receiver, establish a connection to the JMS broker, and
	 * register a message listener with client acknowledgement
	 * 
	 */
	public void onStart() {

		log.info("Initializing the Receiver ...");
		try {
			connection = JMSConnectionFactory.getConnection(brokerURL, userId, password);
			// Set an exception listener
			connection.setExceptionListener(new JMSReceiverExceptionListener(this));
			connection.start();
			log.info("Successfully connected to JMS Broker !");
			/*
			 * Set the acknowledgement as Client acknowledgement, this is
			 * required to ensure Receiver reliability
			 */
			session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			Queue queue = session.createQueue(queueName);
			consumer = session.createConsumer(queue);
			consumer.setMessageListener(new SparkJMSMessageListener(this));
			log.info("Completed startup. Reciever now listening for new messages");
		} catch (JMSException exp) {
			// Caught exception, try a restart
			log.error("Caught exception in startup", exp);
			// Report error to Spark Driver
			reportError("Caught exception in startup", exp);
			restart("Caught exception, restarting.", exp);
		}
	}

	/**
	 * Do cleanup stuff (stop threads, close sockets, etc.) to stop receiving
	 * data
	 */
	public void onStop() {

		log.info("Stopping...");
		try {
			consumer.close();
			session.close();
			connection.close();
		} catch (JMSException exp) {
			log.error("Caught exception stopping", exp);
		}
		log.info("JMS receiver Stopped");
	}
}