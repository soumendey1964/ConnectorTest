package com.paxata.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Suite.class)
@PrepareForTest(DynamoDBUtils.class)
@SuiteClasses({
  ScanAttributesTest.class,
  DynamoDBConnectorFactoryTest.class,
  DynamoDBDataSourceTest.class,
  DynamoDBDataSourceRecordIteratorTest.class
})
public class DynamoDBConnectorTestSuite {

  private static DynamoDBProxyServer dynamoDBProxyServer;
  private static DynamoDB dynamoDB;

  @ClassRule
  public static final ExternalResource resource =
    new ExternalResource() {
      @Override
      protected void before()
        throws Exception {
        dynamoDBProxyServer = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", "8000"});
        dynamoDBProxyServer.start();

        dynamoDB = TestUtils.createDynamoDB();
        TestUtils.createMusicTable(dynamoDB);
        TestUtils.createMusicItems(dynamoDB);
      }

      @Override
      protected void after() {
        try {
          TestUtils.deleteMusicTable(dynamoDB);
        } catch (Exception e) {
          // no-op;
        }

        try {
          dynamoDBProxyServer.stop();
        } catch (Exception e) {
          // no-op;
        }
      }
    };
}
