package com.paxata.connector.dynamodb;

import static com.paxata.connector.dynamodb.TestUtils.TEST_ITEM_NAME;
import static com.paxata.connector.dynamodb.TestUtils.TEST_SAMPLE_ITEMS;
import static org.junit.Assert.assertNotNull;

import com.paxata.connector.dynamodb.AttributeScanner.AttributeType;
import java.util.Map;
import org.junit.Test;

public class ScanAttributesTest {

  @Test
  public void testScanAttributes() {
    Map<String, AttributeType> attributes =
      DynamoDBUtils.scanAttributes(TestUtils.createDynamoDB(), TEST_ITEM_NAME, Integer.valueOf(TEST_SAMPLE_ITEMS));

    assertNotNull(attributes);
  }
}
