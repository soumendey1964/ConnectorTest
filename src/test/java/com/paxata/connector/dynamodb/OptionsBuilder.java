package com.paxata.connector.dynamodb;

import static com.paxata.connector.dynamodb.TestUtils.TEST_AWS_ACCESS_KEY;
import static com.paxata.connector.dynamodb.TestUtils.TEST_AWS_REGION;
import static com.paxata.connector.dynamodb.TestUtils.TEST_AWS_SECRET_KEY;
import static com.paxata.connector.dynamodb.TestUtils.TEST_PROXY_AUTH;
import static com.paxata.connector.dynamodb.TestUtils.TEST_PROXY_HOST;
import static com.paxata.connector.dynamodb.TestUtils.TEST_PROXY_PASSWORD;
import static com.paxata.connector.dynamodb.TestUtils.TEST_PROXY_PORT;
import static com.paxata.connector.dynamodb.TestUtils.TEST_PROXY_SETUP;
import static com.paxata.connector.dynamodb.TestUtils.TEST_PROXY_USER;
import static com.paxata.connector.dynamodb.TestUtils.TEST_SAMPLE_ITEMS;
import static com.paxata.connector.dynamodb.config.AWSAuthType.AWSCredential;
import static com.paxata.connector.dynamodb.config.AWSAuthType.IAMRole;
import static com.paxata.connector.dynamodb.config.Option.AWS_ACCESS_KEY;
import static com.paxata.connector.dynamodb.config.Option.AWS_AUTH_TYPE;
import static com.paxata.connector.dynamodb.config.Option.AWS_REGION;
import static com.paxata.connector.dynamodb.config.Option.AWS_SECRET_KEY;
import static com.paxata.connector.dynamodb.config.Option.PROXY_AUTH;
import static com.paxata.connector.dynamodb.config.Option.PROXY_HOST;
import static com.paxata.connector.dynamodb.config.Option.PROXY_PASSWORD;
import static com.paxata.connector.dynamodb.config.Option.PROXY_PORT;
import static com.paxata.connector.dynamodb.config.Option.PROXY_SETUP;
import static com.paxata.connector.dynamodb.config.Option.PROXY_USER;
import static com.paxata.connector.dynamodb.config.Option.SAMPLE_ITEMS;

import java.util.HashMap;
import java.util.Map;

class OptionsBuilder {

  private Map<String, String> options = new HashMap<>();

  OptionsBuilder addProxySetup() {
    options.put(PROXY_SETUP.getKey(), TEST_PROXY_SETUP);
    return this;
  }

  OptionsBuilder addProxyHost() {
    options.put(PROXY_HOST.getKey(), TEST_PROXY_HOST);
    return this;
  }

  OptionsBuilder addProxyPort() {
    options.put(PROXY_PORT.getKey(), TEST_PROXY_PORT);
    return this;
  }

  OptionsBuilder addProxyAuth() {
    options.put(PROXY_AUTH.getKey(), TEST_PROXY_AUTH);
    return this;
  }

  OptionsBuilder addProxyUser() {
    options.put(PROXY_USER.getKey(), TEST_PROXY_USER);
    return this;
  }

  OptionsBuilder addProxyPassword() {
    options.put(PROXY_PASSWORD.getKey(), TEST_PROXY_PASSWORD);
    return this;
  }

  OptionsBuilder addAWSRegion() {
    options.put(AWS_REGION.getKey(), TEST_AWS_REGION);
    return this;
  }

  OptionsBuilder addAWSCredential() {
    options.put(AWS_AUTH_TYPE.getKey(), AWSCredential.getValue());
    return this;
  }

  OptionsBuilder addAWSIAMRole() {
    options.put(AWS_AUTH_TYPE.getKey(), IAMRole.getValue());
    return this;
  }

  OptionsBuilder addAWSAccessKey() {
    options.put(AWS_ACCESS_KEY.getKey(), TEST_AWS_ACCESS_KEY);
    return this;
  }

  OptionsBuilder addAWSSecretKey() {
    options.put(AWS_SECRET_KEY.getKey(), TEST_AWS_SECRET_KEY);
    return this;
  }

  OptionsBuilder addSampleItems() {
    options.put(SAMPLE_ITEMS.getKey(), TEST_SAMPLE_ITEMS);
    return this;
  }

  Map<String, String> build() {
    return new HashMap<>(options);
  }
}
