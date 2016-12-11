package app;


import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Message Producer
 * Created by Riz
 */
public class Producer {
  MessageProducer producer;
  Session session;
  Connection connection;

  public void sendTextMessage(String aMessage){
    try {
      // Create a messages
      TextMessage message = session.createTextMessage(aMessage);

      // Tell the producer to send the message
      System.out.println("Sent message: " + aMessage);
      producer.send(message);
    }
    catch (Exception ex){
      throw new RuntimeException(aMessage);
    }
  }

  /**
   * @param aBrokerUri Uri to the broker
   * @param aQueue Queue send message
   */
  public Producer(String aBrokerUri, int port,  String aQueue){
    try {

      Map<String,Object> transportParams = new HashMap<>();
      transportParams.put(TransportConstants.PORT_PROP_NAME, port);

      TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams);
      ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,transportConfiguration);

      session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      Destination destination = session.createQueue(aQueue);

      producer = session.createProducer(destination);
      producer.setDeliveryMode(DeliveryMode.PERSISTENT);

    }
    catch (Exception e) {
      System.out.println("Caught: " + e);
      e.printStackTrace();
    }
  }

  public void close(){
    try {
      session.close();
      connection.close();
    }
    catch (Exception ex)
    {
      throw new RuntimeException(ex);
    }
  }
}