package com.paxata.connector.dynamodb.config;

/**
 * DynamoDB connector configuration options to be setup by user.
 */
public enum Option {

  PROXY_SETUP("proxy.setup"),
  PROXY_HOST("proxy.host"),
  PROXY_PORT("proxy.port"),
  PROXY_AUTH("proxy.auth"),
  PROXY_USER("proxy.user"),
  PROXY_PASSWORD("proxy.password"),
  AWS_REGION("aws.region"),
  AWS_AUTH_TYPE("aws.auth.type"),
  AWS_ACCESS_KEY("aws.accessKey"),
  AWS_SECRET_KEY("aws.secretKey"),
  SAMPLE_ITEMS("ddb.table.sampleItems");

  private final String key;

  Option(String key) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }
}