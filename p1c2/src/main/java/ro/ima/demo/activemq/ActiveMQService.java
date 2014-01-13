package ro.ima.demo.activemq;

import java.io.File;
import java.net.URI;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

// http://activemq.apache.org/how-do-i-embed-a-broker-inside-a-connection.html
public class ActiveMQService {

	// // private final String url = "vm://localhost?async=false";
	// // private final String user = ActiveMQConnection.DEFAULT_USER;
	// // private final String password = ActiveMQConnection.DEFAULT_PASSWORD;
	// // private final boolean persistent = true;
	// private final int producerTimeToLive = 0;

	private final BrokerService brokerService;

	// private final Connection connection;

	// private ThreadLocal<Session> sessionTL = new ThreadLocal<Session>();
	// private ThreadLocal<MessageProducer> producerTL = new
	// ThreadLocal<MessageProducer>();
	// private ThreadLocal<MessageConsumer> consumerTL = new
	// ThreadLocal<MessageConsumer>();

	public ActiveMQService(String connectorUri,
			String amqPersistenceAdapterDirectory) throws Exception{// throws Exception {
		// this.queueName = name;

		this.brokerService = new BrokerService();
		this.brokerService.setUseJmx(true);
		// this.brokerService.setUseMirroredQueues(true);

		TransportConnector connector = new TransportConnector();
		connector.setUri(new URI(connectorUri));
		this.brokerService.addConnector(connector);

		// this.brokerService.setPersistenceAdapter(getKahaDBPersistenceAdapter());
		if (amqPersistenceAdapterDirectory != null
				&& !amqPersistenceAdapterDirectory.isEmpty()) {
			this.brokerService.getPersistenceAdapter().setDirectory(
					new File(amqPersistenceAdapterDirectory));
		}

		this.brokerService.setUseShutdownHook(false);
		this.brokerService.setPersistent(true);
		this.brokerService.start();

		// createAndStartJMSConnection(user, password, url);
		/*
		 * ActiveMQConnectionFactory connectionFactory = new
		 * ActiveMQConnectionFactory(user, password, url); this.connection =
		 * connectionFactory.createConnection(); this.connection.start();
		 * 
		 * Session session = connection.createSession(false,
		 * Session.AUTO_ACKNOWLEDGE);
		 * 
		 * Destination destination = session.createQueue(this.queueName);
		 * MessageProducer messageProducer =
		 * session.createProducer(destination);
		 * messageProducer.setDeliveryMode(persistent ?
		 * DeliveryMode.PERSISTENT:DeliveryMode.NON_PERSISTENT);
		 * messageProducer.setTimeToLive(producerTimeToLive);
		 */
	}

	public void stop(){
		try {
			brokerService.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/*
	 * private void createAndStartJMSConnection(String user, String password,
	 * String url) throws JMSException { // Create the connection.
	 * ActiveMQConnectionFactory connectionFactory = new
	 * ActiveMQConnectionFactory(user, password, url); this.connection =
	 * connectionFactory.createConnection(); this.connection.start(); }
	 */

	/*
	 * private Session getSession() throws JMSException { Session session =
	 * sessionTL.get();
	 * 
	 * if (session == null) { session = connection.createSession(false,
	 * Session.AUTO_ACKNOWLEDGE); sessions.add(session); sessionTL.set(session);
	 * }
	 * 
	 * return session; }
	 */

	/*
	 * private MessageProducer getProducer(boolean persistent, long timeToLive)
	 * throws JMSException { Destination destination =
	 * getSession().createQueue(this.queueName); MessageProducer messageProducer
	 * = getSession().createProducer(destination); //
	 * producers.add(messageProducer);
	 * 
	 * if (persistent) {
	 * messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT); } else {
	 * messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT); } if
	 * (timeToLive != 0) { messageProducer.setTimeToLive(timeToLive); }
	 * 
	 * // producerTL.set(messageProducer); }
	 * 
	 * return messageProducer; }
	 */

	@SuppressWarnings("unused")
	private KahaDBPersistenceAdapter getKahaDBPersistenceAdapter() {
		KahaDBPersistenceAdapter persistenceAdapter = new KahaDBPersistenceAdapter();
		persistenceAdapter.setCheckForCorruptJournalFiles(true);
		persistenceAdapter.setIgnoreMissingJournalfiles(true);
		persistenceAdapter.setChecksumJournalFiles(true);
		persistenceAdapter.setConcurrentStoreAndDispatchQueues(false);
		persistenceAdapter.setIndexWriteBatchSize(10);

		// if (storageLocationPath != null &&
		// !"".equals(storageLocationPath.trim())) {
		// File storageLocation = new File(storageLocationPath);
		// persistenceAdapter.setDirectory(storageLocation);
		// }

		return persistenceAdapter;
	}

}
