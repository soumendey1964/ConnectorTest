package com.paxata.connector.dynamodb;

import static com.paxata.connector.spi.config.DataSourceSupportOption.BROWSE;
import static com.paxata.connector.spi.config.DataSourceSupportOption.IMPORT;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.paxata.connector.spi.ChangeSet;
import com.paxata.connector.spi.ConnectorFactory;
import com.paxata.connector.spi.config.ConnectorConfigPhase;
import com.paxata.connector.spi.config.ConnectorConfigurationParser;
import com.paxata.connector.spi.config.ConnectorDefinition;
import com.paxata.connector.spi.config.ConnectorOptionValidationError;
import com.paxata.connector.spi.config.DataSourceSupportOption;
import com.paxata.connector.spi.config.SessionInfo;
import com.paxata.connector.spi.exception.ConnectorException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link ConnectorFactory Connector factory} implementation to connect, browse and import AWS DynamoDB tables.
 * It's sole purpose is to create and return a {@link DynamoDBDataSource AWS DynamoDB "connection"}.
 */
public class DynamoDBConnectorFactory
  implements ConnectorFactory {

  private static final DataSourceSupportOption[] SUPPORTED_FEATURES = {BROWSE, IMPORT};

  /**
   * This implementation supports {@link DataSourceSupportOption#BROWSE} and {@link DataSourceSupportOption#IMPORT}.
   */
  @Override
  public DataSourceSupportOption[] getDataSourceOptions(SessionInfo sessionInfo, Map<String, String> map) {
    return SUPPORTED_FEATURES;
  }

  /**
   * Creates a {@link DynamoDBDataSource DynamoDB data source} using the connector configuration options setup by user.
   */
  @Override
  public DynamoDBDataSource create(SessionInfo sessionInfo, Map<String, String> options) {
    return new DynamoDBDataSource(options);
  }

  @Override
  public List<ConnectorOptionValidationError> validateOptions(SessionInfo sessionInfo, ConnectorConfigPhase phase,
                                                              Map<String, String> options, ConnectorDefinition config) {
    return ConnectorConfigurationParser.validateOptions(phase, options, config);
  }

  @Override
  public void testConnection(SessionInfo sessionInfo, Map<String, String> options) {
    final DynamoDBDataSource dataSource;
    try {
      dataSource = create(sessionInfo, options);
    } catch (SdkClientException e) {
      throw new ConnectorException("test.connection.failed");
    }

    final DynamoDB dynamoDB = dataSource.getDynamoDB();
    final String testConnectionTable = "PaxataConnectorTestConnection";

    try {
      final CreateTableRequest tableRequest =
        new CreateTableRequest().withTableName(testConnectionTable)
                                .withAttributeDefinitions(new AttributeDefinition("ID", "N"))
                                .withKeySchema(new KeySchemaElement("ID", KeyType.HASH))
                                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

      dynamoDB.createTable(tableRequest).waitForActive();
      dynamoDB.getTable(testConnectionTable).delete();
    } catch (AmazonDynamoDBException e) {
      if ("UnrecognizedClientException".equals(e.getErrorCode())
          || "InvalidSignatureException".equals(e.getErrorCode())) {
        throw new ConnectorException("test.connection.failed");
      }

      System.out.println(e.getErrorCode());
      e.printStackTrace();
      // else things should be fine
    } catch (InterruptedException e) {
      // shouldn't happen
      // no-op;
    } finally {
      dataSource.close();
    }
  }

  /**
   * This is the initial version of the connector, it has no change set yet.
   *
   * @return empty collection
   */
  @Override
  public Collection<ChangeSet> getAllChangeSets() {
    return Collections.emptyList();
  }
}