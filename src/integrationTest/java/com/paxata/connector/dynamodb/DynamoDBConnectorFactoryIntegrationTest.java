package com.paxata.connector.dynamodb;


import static com.paxata.connector.dynamodb.IntegrationTestUtils.TEST_OPTIONS;
import static com.paxata.connector.dynamodb.config.Option.AWS_ACCESS_KEY;
import static com.paxata.connector.dynamodb.config.Option.AWS_REGION;
import static com.paxata.connector.dynamodb.config.Option.AWS_SECRET_KEY;
import static org.mockito.Mockito.mock;

import com.google.common.io.Files;
import com.paxata.connector.spi.config.ConnectorConfigurationParser;
import com.paxata.connector.spi.config.ConnectorDefinition;
import com.paxata.connector.spi.config.SessionInfo;
import com.paxata.connector.spi.exception.ConnectorException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DynamoDBConnectorFactoryIntegrationTest {

  private static final Map<String, String> WRONG_OPTIONS = new HashMap<>();

  private static ConnectorDefinition connectorDefinition;

  private static DynamoDBConnectorFactory connectorFactory;
  private static SessionInfo sessionInfo;

  @BeforeClass
  public static void setUp()
    throws Exception {
    connectorDefinition =
      ConnectorConfigurationParser.parse(Files.toString(new File("conf/connector.json"), StandardCharsets.UTF_8))
                                  .connectorDefn();

    connectorFactory = new DynamoDBConnectorFactory();
    sessionInfo = mock(SessionInfo.class);
  }

  @AfterClass
  public static void tearDown() {
  }

  /**
   * Connection test will succeed using AWS Credentials
   */
  @Test
  public void testSuccessfulConnectionAWS() {
    connectorFactory.testConnection(sessionInfo, TEST_OPTIONS);
  }

  /**
   * Connection test will fail with wrong access key.
   */
  @Test(expected = ConnectorException.class)
  public void testUnsuccessfulConnectionWrongAccessKey() {
    WRONG_OPTIONS.putAll(TEST_OPTIONS);
    WRONG_OPTIONS.put(AWS_ACCESS_KEY.getKey(), "wrongAccessKey");
    connectorFactory.testConnection(sessionInfo, WRONG_OPTIONS);
  }

  /**
   * Connection test will fail with wrong secret key.
   */
  @Test(expected = ConnectorException.class)
  public void testUnsuccessfulConnectionWrongSecretKey() {
    WRONG_OPTIONS.putAll(TEST_OPTIONS);
    WRONG_OPTIONS.put(AWS_SECRET_KEY.getKey(), "wrongSecretKey");
    connectorFactory.testConnection(sessionInfo, WRONG_OPTIONS);
  }

  /**
   * Connection test will fail with wrong region.
   */
  @Test(expected = ConnectorException.class)
  public void testUnsuccessfulConnectionWrongRegion() {
    WRONG_OPTIONS.putAll(TEST_OPTIONS);
    WRONG_OPTIONS.put(AWS_REGION.getKey(), "wrongRegion");
    connectorFactory.testConnection(sessionInfo, WRONG_OPTIONS);
  }
}
