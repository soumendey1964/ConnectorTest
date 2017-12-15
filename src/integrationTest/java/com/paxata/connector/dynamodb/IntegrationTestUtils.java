package com.paxata.connector.dynamodb;

import static com.paxata.connector.dynamodb.config.AWSAuthType.AWSCredential;
import static com.paxata.connector.dynamodb.config.Option.AWS_ACCESS_KEY;
import static com.paxata.connector.dynamodb.config.Option.AWS_AUTH_TYPE;
import static com.paxata.connector.dynamodb.config.Option.AWS_REGION;
import static com.paxata.connector.dynamodb.config.Option.AWS_SECRET_KEY;
import static com.paxata.connector.dynamodb.config.Option.PROXY_HOST;
import static com.paxata.connector.dynamodb.config.Option.PROXY_PASSWORD;
import static com.paxata.connector.dynamodb.config.Option.PROXY_PORT;
import static com.paxata.connector.dynamodb.config.Option.PROXY_USER;
import static com.paxata.connector.dynamodb.config.Option.SAMPLE_ITEMS;

import com.paxata.connector.dynamodb.config.Option;
import java.util.HashMap;
import java.util.Map;

public class IntegrationTestUtils {

  static final String TEST_PROXY_SETUP = IntegrationTestOptions.findValue(Option.PROXY_SETUP);
  static final String TEST_PROXY_HOST = IntegrationTestOptions.findValue(Option.PROXY_HOST);
  static final String TEST_PROXY_PORT = IntegrationTestOptions.findValue(Option.PROXY_PORT);
  static final String TEST_PROXY_USER = IntegrationTestOptions.findValue(Option.PROXY_AUTH);
  static final String TEST_PROXY_AUTH = IntegrationTestOptions.findValue(Option.PROXY_USER);
  static final String TEST_PROXY_PASSWORD = IntegrationTestOptions.findValue(Option.PROXY_PASSWORD);
  static final String TEST_AWS_REGION = IntegrationTestOptions.findValue(Option.AWS_REGION);
  static final String TEST_AWS_ACCESS_KEY = IntegrationTestOptions.findValue(Option.AWS_ACCESS_KEY);
  static final String TEST_AWS_SECRET_KEY = IntegrationTestOptions.findValue(Option.AWS_SECRET_KEY);
  static final String TEST_SAMPLE_ITEMS = IntegrationTestOptions.findValue(Option.SAMPLE_ITEMS);

  static final String PATH_SEPARATOR = "/";

  static final String TEST_MIME_TYPE = "application/x.dbtable";

  static final Map<String, String> TEST_OPTIONS = new HashMap<>();

  static {
    TEST_OPTIONS.put(PROXY_HOST.getKey(), TEST_PROXY_HOST);
    TEST_OPTIONS.put(PROXY_PORT.getKey(), TEST_PROXY_PORT);
    TEST_OPTIONS.put(PROXY_USER.getKey(), TEST_PROXY_USER);
    TEST_OPTIONS.put(PROXY_PASSWORD.getKey(), TEST_PROXY_PASSWORD);
    TEST_OPTIONS.put(AWS_REGION.getKey(), TEST_AWS_REGION);
    TEST_OPTIONS.put(AWS_AUTH_TYPE.getKey(), AWSCredential.getValue());
    TEST_OPTIONS.put(AWS_ACCESS_KEY.getKey(), TEST_AWS_ACCESS_KEY);
    TEST_OPTIONS.put(AWS_SECRET_KEY.getKey(), TEST_AWS_SECRET_KEY);
    TEST_OPTIONS.put(SAMPLE_ITEMS.getKey(), TEST_SAMPLE_ITEMS);
    System.out.println("TEST_OPTIONS = " + TEST_OPTIONS);
  }

  private IntegrationTestUtils() {
  }
}
