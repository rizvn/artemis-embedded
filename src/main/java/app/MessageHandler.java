package app;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;

/**
 * Created by Riz
 */
public class MessageHandler implements org.apache.activemq.artemis.api.core.client.MessageHandler{

  ClientSession session;

  public void setSession(ClientSession session){
    this.session = session;
  }

  @Override
  public void onMessage(ClientMessage message) {
      try {
        System.out.println(message.getBodyBuffer().readString());
        message.individualAcknowledge();
        session.commit();
      }
      catch (Exception ex){
        throw new IllegalStateException(ex);
      }
  }
}
