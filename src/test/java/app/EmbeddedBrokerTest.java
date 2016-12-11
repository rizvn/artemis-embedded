package app;

import org.junit.Test;

/**
 * Created by Riz
 */
public class EmbeddedBrokerTest {

  @Test
  public void start() throws Exception{
    EmbeddedBroker app = new EmbeddedBroker();
    app.start();
  }
}