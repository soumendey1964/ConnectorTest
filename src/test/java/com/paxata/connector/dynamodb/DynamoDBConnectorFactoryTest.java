package com.paxata.connector.dynamodb;

import static com.paxata.connector.dynamodb.TestUtils.TEST_OPTIONS;
import static com.paxata.connector.dynamodb.config.Option.AWS_ACCESS_KEY;
import static com.paxata.connector.dynamodb.config.Option.AWS_AUTH_TYPE;
import static com.paxata.connector.dynamodb.config.Option.AWS_REGION;
import static com.paxata.connector.dynamodb.config.Option.AWS_SECRET_KEY;
import static com.paxata.connector.dynamodb.config.Option.PROXY_HOST;
import static com.paxata.connector.dynamodb.config.Option.PROXY_PASSWORD;
import static com.paxata.connector.dynamodb.config.Option.PROXY_PORT;
import static com.paxata.connector.dynamodb.config.Option.PROXY_USER;
import static com.paxata.connector.dynamodb.config.Option.SAMPLE_ITEMS;
import static com.paxata.connector.spi.config.ConnectorConfigPhase.ConnectorPhase;
import static com.paxata.connector.spi.config.ConnectorConfigPhase.DataSourcePhase;
import static com.paxata.connector.spi.config.ConnectorConfigPhase.SessionPhase;
import static com.paxata.connector.spi.config.DataSourceSupportOption.BROWSE;
import static com.paxata.connector.spi.config.DataSourceSupportOption.IMPORT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.google.common.io.Files;
import com.paxata.connector.spi.ChangeSet;
import com.paxata.connector.spi.config.ConnectorConfigurationParser;
import com.paxata.connector.spi.config.ConnectorDefinition;
import com.paxata.connector.spi.config.ConnectorOptionValidationError;
import com.paxata.connector.spi.config.DataSourceSupportOption;
import com.paxata.connector.spi.config.MissingRequiredOption;
import com.paxata.connector.spi.config.SessionInfo;
import com.paxata.connector.spi.exception.ConnectorException;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DynamoDBConnectorFactoryTest {

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

    WRONG_OPTIONS.putAll(TEST_OPTIONS);
    WRONG_OPTIONS.put(AWS_SECRET_KEY.getKey(), "wrongTestSecretKey");
  }

  @AfterClass
  public static void tearDown() {
  }

  /**
   * Tests DataSource options
   */
  @Test
  public void testDataSourceOptions() {
    assertArrayEquals(connectorFactory.getDataSourceOptions(sessionInfo, Collections.<String, String>emptyMap()),
                      new DataSourceSupportOption[]{BROWSE, IMPORT});
  }

  /**
   * Tests DataSource creation, with correct options.
   * Expected behaviour - DataSource will be created and, test connection will succeed.
   */
  @Test
  public void testCreateWithCorrectOptions() {
    // mock the DynamoDB creation
    mockDynamoDBUtils();

    assertNotNull(connectorFactory.create(sessionInfo, TEST_OPTIONS));
    connectorFactory.testConnection(sessionInfo, TEST_OPTIONS);
  }

  /**
   * Tests DataSource creation, with wrong options.
   * Expected behaviour - DataSource will be created however, test connection will fail.
   */
  @Test(expected = ConnectorException.class)
  public void testCreateWithWrongOptions() {
    // mock the DynamoDB creations
    mockDynamoDBUtils();

    assertNotNull(connectorFactory.create(sessionInfo, WRONG_OPTIONS));
    connectorFactory.testConnection(sessionInfo, WRONG_OPTIONS);
  }

  /**
   * Connector phase can have no options set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithNothing() {
    List<ConnectorOptionValidationError> errors =
      connectorFactory
        .validateOptions(sessionInfo, ConnectorPhase, Collections.<String, String>emptyMap(), connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional proxy setup flag set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxySetupFlag() {
    Map<String, String> options = new OptionsBuilder().addProxySetup().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional proxy setup flag, host set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxyHost() {
    Map<String, String> options = new OptionsBuilder().addProxySetup().addProxyHost().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional proxy setup flag, port set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxyPort() {
    Map<String, String> options = new OptionsBuilder().addProxySetup().addProxyPort().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional poxy setup flag, host and port set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxyHostPort() {
    Map<String, String> options = new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional poxy setup flag and auth flag set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxyAuthFlag() {
    Map<String, String> options = new OptionsBuilder().addProxySetup().addProxyAuth().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional proxy host and user set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxyHostUser() {
    Map<String, String> options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyAuth().addProxyUser().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional proxy host, port and user set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxyHostPortUser() {
    Map<String, String> options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().addProxyAuth().addProxyUser().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connector phase can have optional proxy host, port, user and password set.
   */
  @Test
  public void testValidateOptionsInConnectorPhaseWithProxyDetails() {
    Map<String, String> options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().addProxyAuth().addProxyUser()
                          .addProxyPassword().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, ConnectorPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * With none of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithNothing() {
    List<ConnectorOptionValidationError> errors =
      connectorFactory
        .validateOptions(sessionInfo, DataSourcePhase, Collections.<String, String>emptyMap(), connectorDefinition);

    assertEquals(errors.size(), 3);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_REGION.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_AUTH_TYPE.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Also, this phase should have proxy host and port if proxy setup flag was set.
   * With none of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithProxySetupFlag() {
    Map<String, String> options = new OptionsBuilder().addProxySetup().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 5);
    assertTrue(errors.contains(new MissingRequiredOption(PROXY_HOST.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(PROXY_PORT.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_REGION.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_AUTH_TYPE.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Also, this phase should have proxy details if proxy setup flag and auth flag was set.
   * With none of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithProxySetupAuthFlags() {
    Map<String, String> options = new OptionsBuilder().addProxySetup().addProxyAuth().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 7);
    assertTrue(errors.contains(new MissingRequiredOption(PROXY_HOST.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(PROXY_PORT.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(PROXY_USER.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(PROXY_PASSWORD.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_REGION.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_AUTH_TYPE.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * With none of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithProxyDetails() {
    Map<String, String> options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().addProxyAuth().addProxyUser()
                          .addProxyPassword().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 3);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_REGION.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_AUTH_TYPE.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Without any of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithAWSRegion() {
    Map<String, String> options = new OptionsBuilder().addAWSRegion().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 2);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_AUTH_TYPE.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Without any of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithAWSCredential() {
    Map<String, String> options = new OptionsBuilder().addAWSCredential().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 2);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_REGION.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Without any of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithAWSIAMRole() {
    Map<String, String> options = new OptionsBuilder().addAWSIAMRole().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 2);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_REGION.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Without any of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithSampleItems() {
    Map<String, String> options = new OptionsBuilder().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 2);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_REGION.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_AUTH_TYPE.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Without any of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithAWSRegionCredential() {
    Map<String, String> options = new OptionsBuilder().addAWSRegion().addAWSCredential().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 1);
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Without any of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithAWSRegionIAMRole() {
    Map<String, String> options = new OptionsBuilder().addAWSRegion().addAWSIAMRole().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 1);
    assertTrue(errors.contains(new MissingRequiredOption(SAMPLE_ITEMS.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * Without any of these, validation will result in errors.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithAWSRegionSampleItems() {
    Map<String, String> options = new OptionsBuilder().addAWSRegion().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertEquals(errors.size(), 1);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_AUTH_TYPE.getKey())));
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * With all of these, validation will result in success.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithAWSDetailsSampleItems() {
    // with AWS Credential
    Map<String, String> options = new OptionsBuilder().addAWSRegion().addAWSCredential().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());

    // with AWS IAM Role
    options = new OptionsBuilder().addAWSRegion().addAWSIAMRole().addSampleItems().build();
    errors = connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);
    assertTrue(errors.isEmpty());
  }

  /**
   * DataSource phase should have AWS region, AWS authentication type and sample items.
   * With all of these, validation will result in success.
   */
  @Test
  public void testValidateOptionsInDataSourcePhaseWithProxyDetailsAWSDetailsSampleItems() {
    // with AWS Credential
    Map<String, String> options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().addProxyAuth().addProxyUser()
                          .addProxyPassword().addAWSRegion().addAWSCredential().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());

    // with AWS IAM Role
    options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().addProxyAuth().addProxyUser()
                          .addProxyPassword().addAWSRegion().addAWSIAMRole().addSampleItems().build();

    errors = connectorFactory.validateOptions(sessionInfo, DataSourcePhase, options, connectorDefinition);
    assertTrue(errors.isEmpty());
  }

  /**
   * Session phase should have AWS credential details only if AWS Credential was set as AWS authentication type in
   * Connector or DataSource phase.
   * In that case, without this, validation will result in failure.
   *
   * However, session phase shouldn't have any options to set if AWS IAM Role was set as AWS authentication type in
   * Connector or DataSource phase.
   */
  @Test
  public void testValidateOptionInSessionPhaseWithNothing() {
    // with AWS Credential
    Map<String, String> options = new OptionsBuilder().addAWSRegion().addAWSCredential().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);

    assertEquals(errors.size(), 2);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_ACCESS_KEY.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_SECRET_KEY.getKey())));

    // with AWS IAM Role
    options = new OptionsBuilder().addAWSRegion().addAWSIAMRole().addSampleItems().build();
    errors = connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);
    assertTrue(errors.isEmpty());
  }

  /**
   * Session phase should have AWS credential details only if AWS Credential was set as AWS authentication type in
   * Connector or DataSource phase.
   * In that case, without this, validation will result in failure.
   *
   * However, session phase shouldn't have any options to set if AWS IAM Role was set as AWS authentication type in
   * Connector or DataSource phase.
   */
  @Test
  public void testValidateOptionInSessionPhaseWithProxyDetails() {
    // with AWS Credential
    Map<String, String> options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().addProxyAuth().addProxyUser()
                          .addProxyPassword().addAWSRegion().addAWSCredential().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);

    assertEquals(errors.size(), 2);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_ACCESS_KEY.getKey())));
    assertTrue(errors.contains(new MissingRequiredOption(AWS_SECRET_KEY.getKey())));

    // with AWS IAM Role
    options =
      new OptionsBuilder().addProxySetup().addProxyHost().addProxyPort().addProxyAuth().addProxyUser()
                          .addProxyPassword().addAWSRegion().addAWSIAMRole().addSampleItems().build();

    errors = connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);
    assertTrue(errors.isEmpty());
  }

  /**
   * Session phase should have AWS credential details if AWS Credential was set as AWS authentication type in
   * Connector or DataSource phase.
   * In that case, without complete credential details, validation will result in failure.
   */
  @Test
  public void testValidateOptionInSessionPhaseWithAWSAccessKey() {
    Map<String, String> options =
      new OptionsBuilder().addAWSRegion().addAWSCredential().addAWSAccessKey().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);

    assertEquals(errors.size(), 1);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_SECRET_KEY.getKey())));
  }

  /**
   * Session phase should have AWS credential details if AWS Credential was set as AWS authentication type in
   * Connector or DataSource phase.
   * In that case, without complete credential details, validation will result in failure.
   */
  @Test
  public void testValidateOptionInSessionPhaseWithAWSSecretKey() {
    Map<String, String> options =
      new OptionsBuilder().addAWSRegion().addAWSCredential().addAWSSecretKey().addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);

    assertEquals(errors.size(), 1);
    assertTrue(errors.contains(new MissingRequiredOption(AWS_ACCESS_KEY.getKey())));
  }

  /**
   * Session phase should have AWS credential details only if AWS Credential was set as AWS authentication type in
   * Connector or DataSource phase.
   * In that case, with complete credential details, validation will result in success.
   *
   * However, session phase shouldn't have any options to set if AWS IAM Role was set as AWS authentication type in
   * Connector or DataSource phase.
   */
  @Test
  public void testValidateOptionInSessionPhaseWithAWSCredentialDetails() {
    Map<String, String> options =
      new OptionsBuilder().addAWSRegion().addAWSCredential().addAWSAccessKey().addAWSSecretKey().addSampleItems()
                          .build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Session phase should have AWS credential details only if AWS Credential was set as AWS authentication type in
   * Connector or DataSource phase.
   * In that case, with complete credential details, validation will result in success.
   *
   * However, session phase shouldn't have any options to set if AWS IAM Role was set as AWS authentication type in
   * Connector or DataSource phase.
   */
  @Test
  public void testValidateOptionInSessionPhaseWithProxyDetailsAWSCredentialDetails() {
    Map<String, String> options =
      new OptionsBuilder().addProxyAuth().addProxyHost().addProxyPort().addProxySetup().addProxyUser()
                          .addProxyPassword().addAWSRegion().addAWSCredential().addAWSAccessKey().addAWSSecretKey()
                          .addSampleItems().build();

    List<ConnectorOptionValidationError> errors =
      connectorFactory.validateOptions(sessionInfo, SessionPhase, options, connectorDefinition);

    assertTrue(errors.isEmpty());
  }

  /**
   * Connection test will succeed with correct options.
   */
  @Test
  public void testSuccessfulConnection() {
    // mock the DynamoDB creations
    mockDynamoDBUtils();

    connectorFactory.testConnection(sessionInfo, TEST_OPTIONS);
  }

  /**
   * Connection test will fail with wrong options.
   */
  @Test(expected = ConnectorException.class)
  public void testUnsuccessfulConnection() {
    // mock the DynamoDB creations
    mockDynamoDBUtils();

    connectorFactory.testConnection(sessionInfo, WRONG_OPTIONS);
  }

  @Test
  public void testChangeSets() {
    Collection<ChangeSet> allChangeSets = connectorFactory.getAllChangeSets();
    assertNotNull(allChangeSets);
    assertTrue(allChangeSets.isEmpty());
  }

  private void mockDynamoDBUtils() {
    AmazonDynamoDBException dynamoDBException = mock(AmazonDynamoDBException.class);
    when(dynamoDBException.getErrorCode()).thenReturn("InvalidSignatureException");

    DynamoDB wrongDynamoDB = mock(DynamoDB.class);
    when(wrongDynamoDB.createTable(any(CreateTableRequest.class))).thenThrow(dynamoDBException);

    mockStatic(DynamoDBUtils.class);
    when(DynamoDBUtils.createDynamoDB(TEST_OPTIONS)).thenReturn(TestUtils.createDynamoDB());
    when(DynamoDBUtils.createDynamoDB(WRONG_OPTIONS)).thenReturn(wrongDynamoDB);
  }
}
