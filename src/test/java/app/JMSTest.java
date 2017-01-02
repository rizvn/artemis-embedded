package app;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.junit.Test;

import javax.jms.*;

/**
 * Created by Riz
 */
public class JMSTest {

  @Test
  public void testSendMessage() throws Exception{
    TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
    ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,transportConfiguration);

    Queue orderQueue = ActiveMQJMSClient.createQueue("router_inbox");
    Connection connection = cf.createConnection();

    Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    MessageProducer producer = session.createProducer(orderQueue);
    TextMessage message = session.createTextMessage("This is a message for mop");
    message.setStringProperty("toapp","mop");
    producer.send(message);

  }
}
