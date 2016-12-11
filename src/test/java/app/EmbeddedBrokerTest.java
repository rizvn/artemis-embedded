package app;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.*;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by Riz
 */
public class EmbeddedBrokerTest {


  @Test
  public void simpleTest() throws Exception
  {
    Map<String, Object> transportParams = new HashMap<>();
    transportParams.put(TransportConstants.PORT_PROP_NAME, 6161);
    TransportConfiguration connectorConf = new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams);
    TransportConfiguration acceptorConf = new TransportConfiguration(NettyAcceptorFactory.class.getName(), transportParams);
    // TransportConfiguration transportConfiguration = new TransportConfiguration(InVMConnectorFactory.class.getName());


    Configuration embeddedConf = new ConfigurationImpl()
                                  .setPersistenceEnabled(true)
                                  .setJournalDirectory("data/journal")
                                  .setSecurityEnabled(false)
                                  .setJMXManagementEnabled(true)
                                  .setJMXUseBrokerName(true)
                                  .addAcceptorConfiguration(acceptorConf)
                                  .addConnectorConfiguration("connector", connectorConf);

    EmbeddedActiveMQ embedded = new EmbeddedActiveMQ();
    embedded.setConfiguration(embeddedConf);
    embedded.start();


    final String data = "Simple Text " + UUID.randomUUID().toString();
    final String queueName = "simpleQueue";
    final String addressName = "simpleAddress";

    ServerLocator serverLocator = ActiveMQClient.createServerLocator(false, connectorConf);
    ClientSessionFactory sessionFactory = serverLocator.createSessionFactory();
    ClientSession session = sessionFactory.createSession();

    ClientSession.QueueQuery queryResult = session.queueQuery(new SimpleString(queueName));

    // Create a queue bound to a particular address where the test will send to & consume from.
    if (!queryResult.isExists()) {
      session.createQueue(addressName, queueName, true);
    }

    // Create a producer to send a message to the previously created address.
    ClientProducer producer = session.createProducer(addressName);

    // Create a non-durable message.
    ClientMessage message = session.createMessage(false);

    // Put some data into the message.
    message.getBodyBuffer().writeString(data);

    // Send the message. This send will be auto-committed based on the way the session was created in setUp()
    producer.send(message);

    // Close the producer.
    producer.close();

    // Create a consumer on the queue bound to the address where the message was sent.
    ClientConsumer consumer = session.createConsumer(queueName);

    // Start the session to allow messages to be consumed.
    session.start();

    // Receive the message we sent previously.
    message = consumer.receive();

    // Ensure the message was received.
    assertNotNull(message);

    // Acknowledge the message.
    message.acknowledge();

    // Ensure the data in the message received matches the data in the message sent.
    String receivedData = message.getBodyBuffer().readString();
    assertEquals(data, receivedData);
    System.out.println(receivedData);
  }

  @Test
  public void fullTest()throws Exception
  {
    //create broker
    Broker broker = new Broker().setPort(6161).start();

    //define what to do with message when it is received
    MessageHandler messageHandler = new MessageHandler();

    //create message consumer
    Consumer consumer = new Consumer("tcp://localhost:6161", "address1", "queue1", messageHandler);

    //create a message producer
    Producer producer = new Producer("tcp://localhost:6161", "address1", "queue1");

    //create a message every 3 seconds
    while (true){
      producer.sendMessage(LocalDateTime.now().toString() + " Hello world" );
      Thread.sleep(3000);
    }
  }
}