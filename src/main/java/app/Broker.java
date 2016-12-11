package app;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyAcceptorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.NettyConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.netty.TransportConstants;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Riz
 */
public class Broker {

  EmbeddedActiveMQ embedded;
  Integer port = 61617;

  public Broker()
  {

  }

  public Broker setPort(Integer port)
  {
    this.port = port;
    return this;
  }


  public Broker start(){
    try
    {
      Map<String, Object> transportParams = new HashMap<>();
      transportParams.put(TransportConstants.PORT_PROP_NAME, port);
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
      return this;

    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }

  public void stop()
  {
    try {
      embedded.stop();
    }
    catch (Exception ex)
    {
      throw new IllegalStateException(ex);
    }
  }
}
