package com.paxata.connector.dynamodb.config;

public enum AWSAuthType {

  AWSCredential("awsCredential"),
  IAMRole("iamRole");

  private final String value;

  AWSAuthType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}