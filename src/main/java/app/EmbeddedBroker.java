package app;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.ObjectName;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.JMSFactoryType;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptor;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.jms.server.config.ConnectionFactoryConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSConfiguration;
import org.apache.activemq.artemis.jms.server.config.JMSQueueConfiguration;
import org.apache.activemq.artemis.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSConfigurationImpl;
import org.apache.activemq.artemis.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.apache.activemq.artemis.jms.server.embedded.EmbeddedJMS;

/**
 * Created by Riz
 */
public class EmbeddedBroker {

  EmbeddedJMS jmsServer;

  public EmbeddedBroker start() throws Exception{

    // Step 1. Create ActiveMQ Artemis core configuration, and set the properties accordingly
    Map<String,Object> transportParams = new HashMap<>();
    //transportParams.put(TransportConstants.PORT_PROP_NAME, 6161);

    Configuration configuration
      = new ConfigurationImpl()
          .setPersistenceEnabled(true)
          .setJournalDirectory("target/data/journal")
          .setSecurityEnabled(false)
          .addAcceptorConfiguration(
            new TransportConfiguration(NettyAcceptorFactory.class.getName()))
                  .addConnectorConfiguration("connector", new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams));


    // Step 2. Create the JMS configuration
    JMSConfiguration jmsConfig = new JMSConfigurationImpl();

    // Step 3. Configure the JMS ConnectionFactory
    ConnectionFactoryConfiguration cfConfig
      = new ConnectionFactoryConfigurationImpl().setName("cf")
            .setConnectorNames(Arrays.asList("connector")).setBindings("cf");
    jmsConfig.getConnectionFactoryConfigurations().add(cfConfig);


    TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(), transportParams);
    ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,transportConfiguration);

    // Step 4. Configure the JMS Queue
    JMSQueueConfiguration queueConfig= new JMSQueueConfigurationImpl().setName("queue1").setDurable(true).setBindings("queue/queue1");
    jmsConfig.getQueueConfigurations().add(queueConfig);


    // Step 5. Start the JMS Server using the ActiveMQ Artemis core server and the JMS configuration
    jmsServer = new EmbeddedJMS().setConfiguration(configuration).setJmsConfiguration(jmsConfig).start();
    System.out.println("Started Embedded JMS Server");

    // Step 6. Lookup JMS resources defined in the configuration
  //  ConnectionFactory cf = (ConnectionFactory) jmsServer.lookup("cf");
    Queue queue = (Queue) jmsServer.lookup("queue/queue1");

    // Step 7. Send and receive a message using JMS API
    try (Connection connection = cf.createConnection()){
      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageProducer producer = session.createProducer(queue);
      TextMessage message = session.createTextMessage("Hello sent at " + new Date());
      System.out.println("Sending message: " + message.getText());
      producer.send(message);
      MessageConsumer messageConsumer = session.createConsumer(queue);
      connection.start();
      TextMessage messageReceived = (TextMessage) messageConsumer.receive(1000);
      System.out.println("Received message:" + messageReceived.getText());
    } finally {
      // Step 11. Stop the JMS server

    }
    return this;
  }

  public void stop(){
    try{
      jmsServer.stop();
      System.out.println("Stopped the JMS Server");
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }

  }
}
