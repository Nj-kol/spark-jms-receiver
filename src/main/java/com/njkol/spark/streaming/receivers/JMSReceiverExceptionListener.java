package com.njkol.spark.streaming.receivers;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * A JMS exception handler that restarts the Receiver on Exception
 * 
 * @author Nilanjan Sarkar
 *
 */
class JMSReceiverExceptionListener implements ExceptionListener {

	private static final Logger log = LogManager.getRootLogger();
	private Receiver<String> receiver;

	public JMSReceiverExceptionListener(Receiver<String> receiver) {
		this.receiver = receiver;
	}

	public void onException(JMSException exp) {
		log.error("Something went wrong in JMS receiver ...");
		log.error("Connection ExceptionListener fired, attempting restart.", exp);
		receiver.restart("Something went wrong in JMS receiver,now restarting another instance ...", exp);
	}
}