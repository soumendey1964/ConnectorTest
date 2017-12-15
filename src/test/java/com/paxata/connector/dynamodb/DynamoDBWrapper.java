package com.paxata.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchGetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.ListTablesSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import java.util.List;
import java.util.Map;

class DynamoDBWrapper
  extends DynamoDB {

  private DynamoDB dynamoDB;

  DynamoDBWrapper(AmazonDynamoDB client) {
    super(client);
    this.dynamoDB = new DynamoDB(client);
  }

  @Override
  public Table getTable(String tableName) {
    return dynamoDB.getTable(tableName);
  }

  @Override
  public Table createTable(CreateTableRequest req) {
    return dynamoDB.createTable(req);
  }

  @Override
  public Table createTable(String tableName,
                           List<KeySchemaElement> keySchema,
                           List<AttributeDefinition> attributeDefinitions,
                           ProvisionedThroughput provisionedThroughput) {
    return dynamoDB.createTable(tableName, keySchema, attributeDefinitions, provisionedThroughput);
  }

  @Override
  public TableCollection<ListTablesResult> listTables() {
    return dynamoDB.listTables();
  }

  @Override
  public TableCollection<ListTablesResult> listTables(String exclusiveStartTableName) {
    return dynamoDB.listTables(exclusiveStartTableName);
  }

  @Override
  public TableCollection<ListTablesResult> listTables(String exclusiveStartTableName, int maxResultSize) {
    return dynamoDB.listTables(exclusiveStartTableName, maxResultSize);
  }

  @Override
  public TableCollection<ListTablesResult> listTables(int maxResultSize) {
    return dynamoDB.listTables(maxResultSize);
  }

  @Override
  public TableCollection<ListTablesResult> listTables(ListTablesSpec spec) {
    return dynamoDB.listTables(spec);
  }

  @Override
  public BatchGetItemOutcome batchGetItem(ReturnConsumedCapacity returnConsumedCapacity,
                                          TableKeysAndAttributes... tableKeysAndAttributes) {
    return dynamoDB.batchGetItem(returnConsumedCapacity, tableKeysAndAttributes);
  }

  @Override
  public BatchGetItemOutcome batchGetItem(TableKeysAndAttributes... tableKeysAndAttributes) {
    return dynamoDB.batchGetItem(tableKeysAndAttributes);
  }

  @Override
  public BatchGetItemOutcome batchGetItem(BatchGetItemSpec spec) {
    return dynamoDB.batchGetItem(spec);
  }

  @Override
  public BatchGetItemOutcome batchGetItemUnprocessed(ReturnConsumedCapacity returnConsumedCapacity,
                                                     Map<String, KeysAndAttributes> unprocessedKeys) {
    return dynamoDB.batchGetItemUnprocessed(returnConsumedCapacity, unprocessedKeys);
  }

  @Override
  public BatchGetItemOutcome batchGetItemUnprocessed(Map<String, KeysAndAttributes> unprocessedKeys) {
    return dynamoDB.batchGetItemUnprocessed(unprocessedKeys);
  }

  @Override
  public BatchWriteItemOutcome batchWriteItem(TableWriteItems... tableWriteItems) {
    return dynamoDB.batchWriteItem(tableWriteItems);
  }

  @Override
  public BatchWriteItemOutcome batchWriteItem(BatchWriteItemSpec spec) {
    return dynamoDB.batchWriteItem(spec);
  }

  @Override
  public BatchWriteItemOutcome batchWriteItemUnprocessed(Map<String, List<WriteRequest>> unprocessedItems) {
    return dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
  }

  @Override
  public void shutdown() {
    // no-op;
  }
}
