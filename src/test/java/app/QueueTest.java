package app;

import org.junit.Test;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Riz
 */
public class QueueTest {

  ExecutorService mExecutorService = Executors.newFixedThreadPool(20);

  String brokerUri = "tcp://localhost:6161";
  String queue= "TestQueue";

  /**
   * This test creates a broker, consumer and producer
   * producer sends 10 messages which are recieved by the consumer
   * @throws Exception
   */
  @Test
  public void produceAndConsumeTest() throws Exception{
    EmbeddedBroker broker = new EmbeddedBroker().start();
    Consumer consumer     = new Consumer(brokerUri, 6161, queue);
    Producer producer     = new Producer(brokerUri, 6161, queue);

    for (int i = 0; i < 10; i++){
      mExecutorService.execute(() -> producer.sendTextMessage("Hello world "+ new Date().getTime()));
    }

    Thread.sleep(5000);
  }



  /**
   * This tests persistence across broker restarts
   * broker is started, the producer send 10 messages, then broker 1 is shutdown
   * then broker is restarted and a consumer is attached to the queue where the producer sent
   * 10 messages previously. The consumer will consume and print those messages
   * @throws Exception
   */
  @Test
  public void testPersistence() throws Exception{

    //Create broker 1
    EmbeddedBroker broker1 = new EmbeddedBroker().start();
    Producer producer      = new Producer(brokerUri, 6161, queue);

    //send messages
    for (int i = 0; i < 10; i++) {
      mExecutorService.execute(() -> producer.sendTextMessage("Hello world "+ new Date().getTime()));
      System.out.println("Produce: "+ i);
    }
    mExecutorService.awaitTermination(5, TimeUnit.SECONDS);

    //shutdown broker 1
    broker1.stop();

    //create broker 2
    System.out.println("Creating broker 2");
    EmbeddedBroker broker2 = new EmbeddedBroker().start();

    //consume messages
    Consumer consumer = new Consumer(brokerUri, 6161, queue);

    //wait
    Thread.sleep(3000);
  }


  /**
   * For adhoc testing, will instantiate broker and send 10 messages
   * @throws Exception
   */
  @Test
  public void produceOnly() throws Exception {
    EmbeddedBroker embeddedBroker = new EmbeddedBroker().start();
    Producer producer = new Producer(brokerUri, 6161,  queue);

    for (int i = 0; i < 10; i++) {
      mExecutorService.execute(() -> producer.sendTextMessage("Hello world "+ new Date().getTime()));
    }
    mExecutorService.shutdown();
  }

  /**
   * For adhoc testing, will instantiate broker and consume whats on the queue
   * @throws Exception
   */
  @Test
  public void consumeONLY() throws Exception {
    EmbeddedBroker embeddedBroker = new EmbeddedBroker().start();
    Consumer consumer = new Consumer(brokerUri, 6161, queue);
    Thread.sleep(10000);
  }
}
