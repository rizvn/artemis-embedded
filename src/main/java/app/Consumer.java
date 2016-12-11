package app;


import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;
import javax.naming.InitialContext;
import java.lang.IllegalStateException;
import java.util.HashMap;
import java.util.Map;

/**
 * Message consumer
 * Created by Riz
 */
public class Consumer implements MessageListener {
  MessageConsumer mConsumer;
  Session mSession;
  Connection mConnection;

  /**
   * @param brokerUri Uri for the broker
   * @param queueName Queue from which to consume message
   */
  public Consumer(String brokerUri, int port, String queueName){
    try {
      Map<String,Object> transportParams = new HashMap<>();
      transportParams.put(TransportConstants.PORT_PROP_NAME, port);

      TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams);
      ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,transportConfiguration);

      Queue queue = ActiveMQJMSClient.createQueue(queueName);
      mConnection = cf.createConnection();

      mSession = mConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      mConsumer =  mSession.createConsumer(queue);
      mConsumer.setMessageListener(this);
    }
    catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Close consumer
   */
  public void close(){
    try {
      mConsumer.close();
      mSession.close();
      mConnection.close();
    }
    catch (Exception aEx){
      throw new RuntimeException(aEx);
    }
  }

  /**
   * Called when an expection occurs
   * @param ex
   */
  public synchronized void onException(JMSException ex) {
    ex.printStackTrace();
    System.out.println("JMS Exception occured.  Shutting down client.");
  }

  /**
   * Called when a message is received
   * @param aMessage the message
   */
  @Override
  public void onMessage(Message aMessage) {
    try {
      if (aMessage instanceof TextMessage) {
        TextMessage textMessage = (TextMessage) aMessage;
        String text = textMessage.getText();
        System.out.println("Received: " + text);
        //acknowledge that the message has been recieved, so it remove from queue
         aMessage.acknowledge();
      } else {
        System.out.println("Received: " + aMessage);
      }
    }
    catch (Exception ex)
    {
      throw new RuntimeException(ex);
    }
  }
}