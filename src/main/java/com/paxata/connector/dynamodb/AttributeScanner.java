package com.paxata.connector.dynamodb;

import static com.paxata.connector.dynamodb.AttributeScanner.AttributeType.BINARY;
import static com.paxata.connector.dynamodb.AttributeScanner.AttributeType.BOOLEAN;
import static com.paxata.connector.dynamodb.AttributeScanner.AttributeType.NUMBER;
import static com.paxata.connector.dynamodb.AttributeScanner.AttributeType.STRING;
import static com.paxata.connector.dynamodb.AttributeScanner.AttributeType.UNSUPPORTED;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * AWS DynamoDB table's attributes scanner.
 */
class AttributeScanner {

  private static final AttributeScanner DEFAULT_INSTANCE = new AttributeScanner();

  private AttributeScanner() {
  }

  static AttributeScanner getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  /**
   * Scans the DynamoDB table sequentially for it's attributes.
   *
   * @param dynamoDB DynamoDB instance
   * @param tableName DynamoDB table name
   * @param itemLimit number of items to be scanned
   * @return table's attributes
   */
  Map<String, AttributeType> scan(DynamoDB dynamoDB, String tableName, int itemLimit) {
    return scan(dynamoDB, tableName, new ScanSpec(), itemLimit);
  }

  /**
   * Scans the DynamoDB table sequentially with given specification for it's attributes.
   *
   * @param dynamoDB DynamoDB instance
   * @param tableName DynamoDB table name
   * @param scanSpec scan specification
   * @param itemLimit number of items to be scanned
   * @return table's attributes
   */
  private Map<String, AttributeType> scan(DynamoDB dynamoDB, String tableName, ScanSpec scanSpec, int itemLimit) {
    if (itemLimit > 0) {
      scanSpec.setMaxResultSize(itemLimit);
    }

    final Map<String, AttributeType> results = new HashMap<>();

    final ItemCollection<ScanOutcome> items = dynamoDB.getTable(tableName).scan(scanSpec);
    for (Item item : items) {
      final Map<String, Object> attributes = item.asMap();
      for (String name : attributes.keySet()) {
        results.put(name, getDynamoDBType(attributes.get(name)));
      }
    }

    return results;
  }

  /**
   * Derive DynamoDB data type from data value type
   *
   * @param value data value
   * @return {@link AttributeType data type}
   */
  private AttributeType getDynamoDBType(Object value) {
    if (value instanceof String) {
      // string value
      return STRING;
    } else if (value instanceof Number) {
      // number value
      return NUMBER;
    } else if (value instanceof byte[] || value instanceof ByteBuffer) {
      // binary value
      return BINARY;
    } else if (value instanceof Boolean) {
      // boolean value
      return BOOLEAN;
    } else {
      // everything else will not be imported
      // so,
      // let us mark this as UNSUPPORTED
      // and eventually, warn the user
      return UNSUPPORTED;
    }
  }

  public enum AttributeType {STRING, NUMBER, BINARY, BOOLEAN, UNSUPPORTED}
}
