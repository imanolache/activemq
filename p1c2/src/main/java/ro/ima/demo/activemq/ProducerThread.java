/**
 * 
 */
package ro.ima.demo.activemq;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class ProducerThread extends Thread {

	private final int producerTimeToLive = 0;

	private final String user;
	private final String password;
	private final String url;
	private final boolean persistent;
	private final String queueName;

	private boolean running;

	private Connection connection;
	private Session session;
	private MessageProducer messageProducer;

	private long limit;

	// ConnectionInfo: user, password, url
	public ProducerThread(String user, String password, String url,
			String queueName, boolean persistent, long limit) {
		this.user = user;
		this.password = password;
		this.url = url;
		this.queueName = queueName;
		this.persistent = persistent;
		this.limit = limit;
	}

	public void terminate() {
		this.running = false;
	}

	private void closeMessageProducer() {
		try {
			if (this.messageProducer != null) {
				this.messageProducer.close();
			}
			if (this.session != null) {
				this.session.close();
			}
			if (this.messageProducer != null) {
				this.messageProducer.close();
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	private void createMessageProducer() throws JMSException {
		System.out.println("entering createMessageProducer");
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				user, password, url);
		connection = connectionFactory.createConnection();
		connection.start();

		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination destination = session.createQueue(this.queueName);
		messageProducer = session.createProducer(destination);
		messageProducer.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT
				: DeliveryMode.NON_PERSISTENT);
		messageProducer.setTimeToLive(producerTimeToLive);
	}

	public void run() {
		int index = 0;
		long ts = System.currentTimeMillis();
		try {
			createMessageProducer();

			Thread.currentThread().setName(queueName + "-ProducerThread");

			System.out.println("start ProducerThread");

			running = true;

			while (running) {
				ITask jmsMessage = new JmsTask("jms task " + (++index));

				// System.out.println(message.toString());

				ObjectMessage o = null;

				try {
					o = session.createObjectMessage(jmsMessage);
					messageProducer.send(o);
				} catch (JMSException ex) {
					ex.printStackTrace();
				}

				if (limit > 0 && index > limit) {
					running = false;
				}
			}
		} catch (JMSException e) {
			e.printStackTrace();
		}

		long te = System.currentTimeMillis();
		System.out.println("stop ProducerThread, " + index + " messages, "
				+ (te - ts) / 1000 + " seconds");

		closeMessageProducer();
	}
}
