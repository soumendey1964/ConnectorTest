package com.paxata.connector.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.paxata.connector.dynamodb.AttributeScanner.AttributeType;
import com.paxata.connector.dynamodb.config.AWSAuthType;
import com.paxata.connector.dynamodb.config.OptionUtils;
import java.util.Map;

final class DynamoDBUtils {

  private DynamoDBUtils() {
  }

  static DynamoDB createDynamoDB(Map<String, String> options) {
    final ClientConfiguration configuration = new ClientConfiguration();

    if (OptionUtils.hasProxyHost(options)) {
      configuration.setProxyHost(OptionUtils.getProxyHost(options));
    }
    if (OptionUtils.hasProxyPort(options)) {
      configuration.setProxyPort(Integer.valueOf(OptionUtils.getProxyPort(options)));
    }
    if (OptionUtils.hasProxyUser(options)) {
      configuration.setProxyUsername(OptionUtils.getProxyUser(options));
    }
    if (OptionUtils.hasProxyPassword(options)) {
      configuration.setProxyPassword(OptionUtils.getProxyPassword(options));
    }

    final String awsAuthType = OptionUtils.getAWSAuthType(options);
    AWSCredentialsProvider credentialsProvider = null;
    if (awsAuthType.equals(AWSAuthType.AWSCredential.getValue())) {
      final AWSCredentials credentials =
        new BasicAWSCredentials(OptionUtils.getAWSAccessKey(options), OptionUtils.getAWSSecretKey(options));
      credentialsProvider = new AWSStaticCredentialsProvider(credentials);
    } else if (awsAuthType.equals(AWSAuthType.IAMRole.toString())) {
      credentialsProvider = new InstanceProfileCredentialsProvider(false);
    }

    final AmazonDynamoDB client =
      AmazonDynamoDBClientBuilder.standard()
                                 .withClientConfiguration(configuration)
                                 .withCredentials(credentialsProvider)
                                 .withRegion(OptionUtils.getAWSRegion(options))
                                 .build();

    return new DynamoDB(client);
  }

  static Map<String, AttributeType> scanAttributes(DynamoDB dynamoDB, String tableName, int itemLimit) {
    return AttributeScanner.getDefaultInstance().scan(dynamoDB, tableName, itemLimit);
  }
}