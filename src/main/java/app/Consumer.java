package app;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;

/**
 * Created by Riz
 */
public class Consumer {

  ClientConsumer consumer;
  ClientSession session;
  String addressName;
  String queuName;
  MessageHandler messageHandler;

  public Consumer(String uri, String addressName, String queueName, MessageHandler messageHandler){
    try {
      this.addressName = addressName;
      this.queuName = queueName;
      this.messageHandler = messageHandler;

      ServerLocator serverLocator = ActiveMQClient.createServerLocator(uri);
      ClientSessionFactory sessionFactory = serverLocator.createSessionFactory();

      session = sessionFactory.createSession(false, false);
      messageHandler.setSession(session);

      // Create a queue bound to a particular address where the test will send to & consume from.

      //query if queue exists
      ClientSession.QueueQuery queryResult = session.queueQuery(new SimpleString(queueName));
      if (!queryResult.isExists()) {
        session.createQueue(addressName, queueName, true);
      }
      consumer = session.createConsumer(queueName);
      consumer.setMessageHandler(this.messageHandler);

      // Start the session to allow messages to be consumed.
      session.start();
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  public void close(){
    try {
      consumer.close();
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  public ClientSession getSession() {
    return session;
  }
}
