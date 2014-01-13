package ro.ima.demo.activemq;

import java.io.InterruptedIOException;
import java.util.HashSet;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class ConsumerPool {

	private String user;
	private String password;
	private String url;
	private String queueName;

	private Connection connection;
	private final int consumerCount;
	private final HashSet<ConsumerWorker> consumerWorkers = new HashSet<ConsumerWorker>();

	public ConsumerPool(String user, String password, String url,
			String queueName, int consumerCount) {
		this.user = user;
		this.password = password;
		this.url = url;
		this.queueName = queueName;
		this.consumerCount = consumerCount;
	}

	public void terminate() {
		System.out.println("entering ConsumerPool.terminate");

		for (ConsumerWorker worker : consumerWorkers) {
			worker.terminate();
		}

		closeConnection();

		System.out.println("exiting ConsumerPool.terminate");
	}

	private void closeConnection() {
		System.out.println("entering ConsumerPool.closeConnection");

		try {
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}

		System.out.println("exiting ConsumerPool.closeConnection");
	}

	private void createConnection() throws JMSException {
		System.out.println("entering ConsumerPool.createConnection");

		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
				user, password, url);
		connection = connectionFactory.createConnection();
		connection.start();

		System.out.println("exiting ConsumerPool.createConnection");
	}

	public void run() {
		System.out.println("entering ConsumerPool.run");
		try {
			createConnection();
		} catch (JMSException e) {
			e.printStackTrace();
		}

		try {
			for (int i = 0; i < consumerCount; i++) {
				consumerWorkers.add(new ConsumerWorker(connection.createSession(false,
						Session.AUTO_ACKNOWLEDGE), this.queueName, i + 1));
			}

		} catch (JMSException e) {
			e.printStackTrace();
			closeConnection();
		}

		for (ConsumerWorker worker : consumerWorkers) {
			worker.start();
		}

		System.out.println("exiting ConsumerPool.run");
	}

}

class ConsumerWorker extends Thread {

	private final Session session;
	private final MessageConsumer messageConsumer;

	private boolean running;
	private String queueName;

	private final String workerName;

	public ConsumerWorker(Session session, String queueName, int index)
			throws JMSException {
		this.workerName = queueName + "-ConsumerWorker-" + index;
		this.session = session;
		this.queueName = queueName;

		Destination destination = session.createQueue(this.queueName);
		this.messageConsumer = session.createConsumer(destination);
	}

	public void terminate() {
		System.out.println("terminate worker=[" + this.getName() + "]");
		this.running = false;
	}

	private void closeMessageConsumer() {
		System.out.println("entering ConsumerWorker.closeMessageConsumer");

		if (messageConsumer != null) {
			try {
				messageConsumer.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

		if (session != null) {
			try {
				session.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

		System.out.println("exiting ConsumerWorker.closeMessageConsumer");
	}

	private Message getTask() throws InterruptedException, JMSException {
		Message message;
		try {
			message = messageConsumer.receive();
		} catch (JMSException e) {

			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException(
						"Interrupted while reading message from JMS queue");
			}

			if (e.getCause() != null
					&& e.getCause() instanceof InterruptedIOException) {
				throw new InterruptedException(
						"Interrupted while reading message from JMS queue: "
								+ e.getMessage());
			}

			throw e;
		}

		return message;
	}

	public void run() {
		Thread.currentThread().setName(this.workerName);

		this.running = true;

		System.out.println("start " + this.workerName);

		while (running) {
			try {
				Message task = getTask();
				if (task != null) {
					((ITask) ((ObjectMessage) task).getObject()).run();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

		System.out.println("stop " + this.workerName);

		this.closeMessageConsumer();
	}
}
