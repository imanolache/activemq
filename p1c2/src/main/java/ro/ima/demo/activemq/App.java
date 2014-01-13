package ro.ima.demo.activemq;

import org.apache.activemq.ActiveMQConnection;

public class App {

	public static void main(String[] args) throws Exception {
		final String connectorUri = "vm://localhost?async=false";
		final String user = ActiveMQConnection.DEFAULT_USER;
		final String password = ActiveMQConnection.DEFAULT_PASSWORD;
		final String amqPersistenceAdapterDirectory = "d:/ActiveMQ";

		System.out.println("Hello World ActiveMQ!");

		final ActiveMQService helloAMQservice = new ActiveMQService(connectorUri,
				amqPersistenceAdapterDirectory);

		final ProducerThread jmsProducerThread = new ProducerThread(user, password,
				connectorUri, "HelloMQ", true, 10000);
		
		final ConsumerPool consumers = new ConsumerPool(user, password,
				connectorUri, "HelloMQ", 10);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void start() {
				super.start();
			}

			@Override
			public void run() {
				System.out.println("run shutdownHook");
				jmsProducerThread.terminate();
				consumers.terminate();
				helloAMQservice.stop();
			}
		});

		jmsProducerThread.start();
		consumers.run();
	}
}
