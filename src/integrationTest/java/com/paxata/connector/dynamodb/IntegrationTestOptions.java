package com.paxata.connector.dynamodb;

import com.paxata.connector.dynamodb.config.Option;
import java.io.IOException;
import java.util.Properties;

class IntegrationTestOptions {

  private static final String DEFAULT_OPTIONS_LOCATION = "options.properties";
  private static final Properties DEFAULT_OPTIONS = new Properties();

  static {
    try {
      DEFAULT_OPTIONS.load(IntegrationTestOptions.class.getClassLoader().getResourceAsStream(DEFAULT_OPTIONS_LOCATION));
    } catch (IOException e) {
      // shouldn't happen
      throw new RuntimeException(e);
    }
  }

  private IntegrationTestOptions() {
  }

  static String findValue(Option option) {
    // find from environment
    // fall back to default options
    String key = option.getKey();
    String value = System.getenv(key);
    return value != null ? value : DEFAULT_OPTIONS.getProperty(key);
  }
}
