package com.paxata.connector.dynamodb;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.paxata.connector.dynamodb.config.OptionUtils;
import org.junit.Test;

public class OptionUtilsTest {

  @Test
  public void testHasProxySetup() {
    assertFalse(OptionUtils.hasProxySetup(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasProxySetup(new OptionsBuilder().addProxySetup().build()));
  }

  @Test
  public void testGetProxySetup() {
    assertNull(OptionUtils.getProxySetup(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getProxySetup(new OptionsBuilder().addProxySetup().build()));
  }

  @Test
  public void testHasProxyHost() {
    assertFalse(OptionUtils.hasProxyHost(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasProxyHost(new OptionsBuilder().addProxyHost().build()));
  }

  @Test
  public void testGetProxyHost() {
    assertNull(OptionUtils.getProxyHost(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getProxyHost(new OptionsBuilder().addProxyHost().build()));
  }

  @Test
  public void testHasProxyPort() {
    assertFalse(OptionUtils.hasProxyPort(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasProxyPort(new OptionsBuilder().addProxyPort().build()));
  }

  @Test
  public void testGetProxyPort() {
    assertNull(OptionUtils.getProxyPort(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getProxyPort(new OptionsBuilder().addProxyPort().build()));
  }

  @Test
  public void testHasProxyAuth() {
    assertFalse(OptionUtils.hasProxyAuth(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasProxyAuth(new OptionsBuilder().addProxyAuth().build()));
  }

  @Test
  public void testGetProxyAuth() {
    assertNull(OptionUtils.getProxyAuth(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getProxyAuth(new OptionsBuilder().addProxyAuth().build()));
  }

  @Test
  public void testHasProxyUser() {
    assertFalse(OptionUtils.hasProxyUser(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasProxyUser(new OptionsBuilder().addProxyUser().build()));
  }

  @Test
  public void testGetProxyUser() {
    assertNull(OptionUtils.getProxyUser(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getProxyUser(new OptionsBuilder().addProxyUser().build()));
  }

  @Test
  public void testHasProxyPassword() {
    assertFalse(OptionUtils.hasProxyPassword(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasProxyPassword(new OptionsBuilder().addProxyPassword().build()));
  }

  @Test
  public void testGetProxyPassword() {
    assertNull(OptionUtils.getProxyPassword(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getProxyPassword(new OptionsBuilder().addProxyPassword().build()));
  }

  @Test
  public void testHasAWSRegion() {
    assertFalse(OptionUtils.hasAWSRegion(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasAWSRegion(new OptionsBuilder().addAWSRegion().build()));
  }

  @Test
  public void testGetAWSRegion() {
    assertNull(OptionUtils.getAWSRegion(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getAWSRegion(new OptionsBuilder().addAWSRegion().build()));
  }

  @Test
  public void testHasAWSAuthType() {
    assertFalse(OptionUtils.hasAWSAuthType(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasAWSAuthType(new OptionsBuilder().addAWSCredential().build()));
    assertTrue(OptionUtils.hasAWSAuthType(new OptionsBuilder().addAWSIAMRole().build()));
  }

  @Test
  public void testGetAWSAuthType() {
    assertNull(OptionUtils.getAWSAuthType(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getAWSAuthType(new OptionsBuilder().addAWSCredential().build()));
    assertNotNull(OptionUtils.getAWSAuthType(new OptionsBuilder().addAWSIAMRole().build()));
  }

  @Test
  public void testHasAWSAccessKey() {
    assertFalse(OptionUtils.hasAWSAccessKey(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasAWSAccessKey(new OptionsBuilder().addAWSAccessKey().build()));
  }

  @Test
  public void testGetAWSAccessKey() {
    assertNull(OptionUtils.getAWSAccessKey(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getAWSAccessKey(new OptionsBuilder().addAWSAccessKey().build()));
  }

  @Test
  public void testHasAWSSecretKey() {
    assertFalse(OptionUtils.hasAWSSecretKey(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasAWSSecretKey(new OptionsBuilder().addAWSSecretKey().build()));
  }

  @Test
  public void testGetAWSSecretKey() {
    assertNull(OptionUtils.getAWSSecretKey(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getAWSSecretKey(new OptionsBuilder().addAWSSecretKey().build()));
  }

  @Test
  public void testHasSampleItems() {
    assertFalse(OptionUtils.hasSampleItems(new OptionsBuilder().build()));
    assertTrue(OptionUtils.hasSampleItems(new OptionsBuilder().addSampleItems().build()));
  }

  @Test
  public void testGetSampleItems() {
    assertNull(OptionUtils.getSampleItems(new OptionsBuilder().build()));
    assertNotNull(OptionUtils.getSampleItems(new OptionsBuilder().addSampleItems().build()));
  }
}
