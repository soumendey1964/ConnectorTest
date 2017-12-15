package com.paxata.connector.dynamodb.config;

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

import com.google.common.base.Strings;
import java.util.Map;

/**
 * Utility class to access DynamoDB connector configuration options setup by user.
 */
public final class OptionUtils {

  private OptionUtils() {
  }

  public static boolean hasProxySetup(Map<String, String> options) {
    return hasOption(options, PROXY_SETUP);
  }

  public static String getProxySetup(Map<String, String> options) {
    return getOption(options, PROXY_SETUP);
  }

  public static boolean hasProxyHost(Map<String, String> options) {
    return hasOption(options, PROXY_HOST);
  }

  public static String getProxyHost(Map<String, String> options) {
    return getOption(options, PROXY_HOST);
  }

  public static boolean hasProxyPort(Map<String, String> options) {
    return hasOption(options, PROXY_PORT);
  }

  public static String getProxyPort(Map<String, String> options) {
    return getOption(options, PROXY_PORT);
  }

  public static boolean hasProxyAuth(Map<String, String> options) {
    return hasOption(options, PROXY_AUTH);
  }

  public static String getProxyAuth(Map<String, String> options) {
    return getOption(options, PROXY_AUTH);
  }

  public static boolean hasProxyUser(Map<String, String> options) {
    return hasOption(options, PROXY_USER);
  }

  public static String getProxyUser(Map<String, String> options) {
    return getOption(options, PROXY_USER);
  }

  public static boolean hasProxyPassword(Map<String, String> options) {
    return hasOption(options, PROXY_PASSWORD);
  }

  public static String getProxyPassword(Map<String, String> options) {
    return getOption(options, PROXY_PASSWORD);
  }

  public static boolean hasAWSRegion(Map<String, String> options) {
    return hasOption(options, AWS_REGION);
  }

  public static String getAWSRegion(Map<String, String> options) {
    return getOption(options, AWS_REGION);
  }

  public static boolean hasAWSAuthType(Map<String, String> options) {
    return hasOption(options, AWS_AUTH_TYPE);
  }

  public static String getAWSAuthType(Map<String, String> options) {
    return getOption(options, AWS_AUTH_TYPE);
  }

  public static boolean hasAWSAccessKey(Map<String, String> options) {
    return hasOption(options, AWS_ACCESS_KEY);
  }

  public static String getAWSAccessKey(Map<String, String> options) {
    return getOption(options, AWS_ACCESS_KEY);
  }

  public static boolean hasAWSSecretKey(Map<String, String> options) {
    return hasOption(options, AWS_SECRET_KEY);
  }

  public static String getAWSSecretKey(Map<String, String> options) {
    return getOption(options, AWS_SECRET_KEY);
  }

  public static boolean hasSampleItems(Map<String, String> options) {
    return hasOption(options, Option.SAMPLE_ITEMS);
  }

  public static String getSampleItems(Map<String, String> options) {
    return getOption(options, Option.SAMPLE_ITEMS);
  }

  private static boolean hasOption(Map<String, String> options, Option option) {
    return !Strings.isNullOrEmpty(options.get(option.getKey()));
  }

  private static String getOption(Map<String, String> options, Option option) {
    return options.get(option.getKey());
  }
}