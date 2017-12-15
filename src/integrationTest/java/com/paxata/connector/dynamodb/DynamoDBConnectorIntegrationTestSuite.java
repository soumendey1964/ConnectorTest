package com.paxata.connector.dynamodb;

import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
  DynamoDBConnectorFactoryIntegrationTest.class
})
public class DynamoDBConnectorIntegrationTestSuite {

  @ClassRule
  public static final ExternalResource resource =
    new ExternalResource() {
      @Override
      protected void before() {
      }

      @Override
      protected void after() {
      }
    };
}
