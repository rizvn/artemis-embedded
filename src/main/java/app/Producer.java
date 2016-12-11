package app;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;

/**
 * Created by Riz
 */
public class Producer {

  ClientProducer producer;
  ClientSession session;
  String addressName;
  String queuName;

  public Producer(String uri, String addressName, String queueName){
    try {
      this.addressName = addressName;
      this.queuName = queueName;
      ServerLocator serverLocator = ActiveMQClient.createServerLocator(uri);
      ClientSessionFactory sessionFactory = serverLocator.createSessionFactory();

      session = sessionFactory.createSession();
      // Create a producer to send a message to the previously created address.
      producer = session.createProducer(addressName);


      // Create a queue bound to a particular address where the test will send to & consume from.

      //query if queue exists
      ClientSession.QueueQuery queryResult = session.queueQuery(new SimpleString(queueName));
      if (!queryResult.isExists()) {
        session.createQueue(addressName, queueName, true);
      }

    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  public void sendMessage(String messageBody){
    try {

      // Create a non-durable message.
      ClientMessage message = session.createMessage(true);

      // Put some data into the message.
      message.getBodyBuffer().writeString(messageBody);


      // Send the message. This send will be auto-committed based on the way the session was created in setUp()
      producer.send(message);
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }
}
