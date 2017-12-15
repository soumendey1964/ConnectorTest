package com.paxata.connector.dynamodb;

import static org.junit.Assert.assertNotNull;

import java.util.Map;
import org.junit.Test;

public class CreateDynamoDBTest {

  @Test
  public void testCreateDynamoDB() {
    Map<String, String> options =
      new OptionsBuilder().addAWSRegion().addAWSCredential().addAWSAccessKey().addAWSSecretKey().build();

    assertNotNull(DynamoDBUtils.createDynamoDB(options));
  }
}
