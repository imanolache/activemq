package ro.ima.demo.activemq;

public class JmsTask implements ITask {

	private static final long serialVersionUID = 1L;
	
	private final String message;

	public JmsTask(String message){
		this.message = message;
	}

	public void run() {
		System.out.println(this.message);
//		try {
//			Thread.sleep(100);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
	}

	@Override
	public String toString() {
		return "HelloMessage [message=" + message + "]";
	}
	
}
