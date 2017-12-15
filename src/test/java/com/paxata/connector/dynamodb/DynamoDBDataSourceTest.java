package com.paxata.connector.dynamodb;

import static com.paxata.connector.dynamodb.TestUtils.PATH_SEPARATOR;
import static com.paxata.connector.dynamodb.TestUtils.TEST_ITEM_NAME;
import static com.paxata.connector.dynamodb.TestUtils.TEST_ITEM_PATH;
import static com.paxata.connector.dynamodb.TestUtils.TEST_MIME_TYPE;
import static com.paxata.connector.dynamodb.TestUtils.TEST_OPTIONS;
import static com.paxata.connector.dynamodb.TestUtils.TEST_SAMPLE_ITEMS;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.paxata.connector.dynamodb.AttributeScanner.AttributeType;
import com.paxata.connector.spi.config.SessionInfo;
import com.paxata.connector.spi.exception.ConnectorException;
import com.paxata.connector.spi.item.DirectoryItem;
import com.paxata.connector.spi.item.ItemRef;
import com.paxata.connector.spi.item.RecordItem;
import com.paxata.connector.spi.log.ConnectorMessages;
import com.paxata.connector.spi.value.ItemSchema;
import com.paxata.connector.spi.value.PrimitiveItemSchema;
import java.util.List;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class DynamoDBDataSourceTest {

  private static DynamoDBDataSource dataSource;

  @BeforeClass
  public static void setUp() {
    mockStatic(DynamoDBUtils.class);
    when(DynamoDBUtils.createDynamoDB(TEST_OPTIONS)).thenReturn(TestUtils.createDynamoDB());
    when(DynamoDBUtils.scanAttributes(any(DynamoDB.class), anyString(), anyInt()))
      .thenAnswer(new Answer<Map<String, AttributeType>>() {
        @Override
        public Map<String, AttributeType> answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          DynamoDB dynamoDB = (DynamoDB) args[0];
          String tableName = (String) args[1];
          int itemLimit = (Integer) args[2];
          return AttributeScanner.getDefaultInstance().scan(dynamoDB, tableName, itemLimit);
        }
      });

    dataSource = new DynamoDBConnectorFactory().create(mock(SessionInfo.class), TEST_OPTIONS);
  }

  @AfterClass
  public static void tearDown() {
  }

  @Test
  public void testGetRootPath() {
    assertEquals(dataSource.getRootPath(), PATH_SEPARATOR);
  }

  @Test
  public void testGetRootItem() {
    assertEquals(dataSource.getRootItem(), new DirectoryItem(PATH_SEPARATOR, PATH_SEPARATOR, false));
  }

  /**
   * We are expecting only one child and that is 'Music' table.
   */
  @Test
  public void testGetChildItems() {
    assertChildItems(dataSource.getChildItems(PATH_SEPARATOR, 0, -1));
  }

  /**
   * We shouldn't be getting any child as we have only one child and that is 'Music' table.
   */
  @Test
  public void testGetChildItemsWithSkip() {
    ItemRef[] childItems = dataSource.getChildItems(PATH_SEPARATOR, 1, -1);

    assertEquals(0, childItems.length);
  }

  /**
   * We shouldn't be getting any child as we have only one child and that is 'Music' table.
   */
  @Test
  public void testGetChildItemsWithZeroLimit() {
    ItemRef[] childItems = dataSource.getChildItems(PATH_SEPARATOR, 0, 0);

    assertEquals(0, childItems.length);
  }

  /**
   * We should be getting 'Music' table as we have only one child and that is 'Music' table.
   */
  @Test
  public void testGetChildItemsWithLimit() {
    assertChildItems(dataSource.getChildItems(PATH_SEPARATOR, 0, 101));
  }

  /**
   * We shouldn't be getting any child as we have only one child and that is 'Music' table.
   */
  @Test
  public void testGetChildItemsWithSkipAndLimit() {
    ItemRef[] childItems = dataSource.getChildItems(PATH_SEPARATOR, 1, 101);

    assertEquals(0, childItems.length);
  }

  @Test(expected = ConnectorException.class)
  public void testGetChildItemsWithInvalidDirectoryPath() {
    dataSource.getChildItems(TEST_ITEM_PATH, 0, -1);
  }

  /**
   * We should be getting 'Music' table schema.
   * However, fields should include 'Reviews' column as it has compound data type. Rather, we should be expecting
   * a log item for it.
   *
   * Additionally, the 'Music' table schema should be cached for future use.
   */
  @Test
  public void testGetItem() {
    RecordItem recordItem = dataSource.getItem(TEST_ITEM_PATH).asRecordItem();
    List<ItemSchema> fields = recordItem.getSchema().getSchema().getFields();
    List<ConnectorMessages> logItems = recordItem.getSchema().getLogItems();

    PrimitiveItemSchema artist = PrimitiveItemSchema.makeText("Artist", false, 400 * 1024);
    PrimitiveItemSchema songTitle = PrimitiveItemSchema.makeText("SongTitle", false, 400 * 1024);
    PrimitiveItemSchema album = PrimitiveItemSchema.makeText("Album", false, 400 * 1024);
    PrimitiveItemSchema genre = PrimitiveItemSchema.makeText("Genre", false, 400 * 1024);
    PrimitiveItemSchema track = PrimitiveItemSchema.makeNumber("Track", false, 38, 38);
    PrimitiveItemSchema composer = PrimitiveItemSchema.makeText("Composer", false, 400 * 1024);
    PrimitiveItemSchema favorite = PrimitiveItemSchema.makeBoolean("Favorite", false);
    PrimitiveItemSchema codec1 = PrimitiveItemSchema.makeBinary("CODEC1", false);
    PrimitiveItemSchema codec2 = PrimitiveItemSchema.makeBinary("CODEC2", false);

    assertEquals(6, recordItem.getRowCount());

    assertEquals(9, fields.size());
    assertTrue(fields.contains(artist));
    assertTrue(fields.contains(songTitle));
    assertTrue(fields.contains(album));
    assertTrue(fields.contains(genre));
    assertTrue(fields.contains(track));
    assertTrue(fields.contains(composer));
    assertTrue(fields.contains(favorite));
    assertTrue(fields.contains(codec1));
    assertTrue(fields.contains(codec2));

    assertEquals(3, logItems.size());

    // subsequent calls (within expiry time) to dataSource.getItem() with same parameter
    // should use the cache built during first call
    mockStatic(DynamoDBUtils.class);
    when(DynamoDBUtils.scanAttributes(dataSource.getDynamoDB(), TEST_ITEM_PATH, Integer.valueOf(TEST_SAMPLE_ITEMS)))
      .thenThrow(UnsupportedOperationException.class);

    dataSource.getItem(TEST_ITEM_PATH);
  }

  @Test(expected = UncheckedExecutionException.class)
  public void testGetItemWithInvalidItemPath() {
    dataSource.getItem(PATH_SEPARATOR);
  }

  /**
   * We should get a non-null iterator for the 'Music' table.
   */
  @Test
  public void testOpenRecordIterator() {
    assertNotNull(dataSource.openRecordIterator(TEST_ITEM_PATH, 0, 101, null));
  }

  @Test(expected = UncheckedExecutionException.class)
  public void testOpenRecordIteratorWithInvalidItemPath() {
    assertNotNull(dataSource.openRecordIterator(PATH_SEPARATOR, 0, 101, null));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateRecordItem() {
    dataSource.createRecordItem(PATH_SEPARATOR, TEST_ITEM_NAME,
                                dataSource.getItem(TEST_ITEM_PATH).asRecordItem().getSchema().getSchema(),
                                TEST_MIME_TYPE);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOpenInputStreamWithOutPreview() {
    dataSource.openInputStream(TEST_ITEM_PATH, false);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOpenInputStreamWithPreview() {
    dataSource.openInputStream(TEST_ITEM_PATH, true);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateStreamItem() {
    dataSource.createStreamItem(PATH_SEPARATOR, TEST_ITEM_NAME, "UTF-8", TEST_MIME_TYPE);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetQueryResultsSchema() {
    dataSource.getQueryResultsSchema("");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOpenQueryResultsIterator() {
    dataSource.openQueryResultsIterator("");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testCreateChildDirectory() {
    dataSource.createChildDirectory(PATH_SEPARATOR, TEST_ITEM_NAME);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testMove() {
    dataSource.move(TEST_ITEM_PATH, PATH_SEPARATOR + "Songs");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDelete() {
    dataSource.delete(TEST_ITEM_PATH);
  }

  private void assertChildItems(ItemRef[] childItems) {
    assertEquals(1, childItems.length);
    assertEquals(TEST_ITEM_PATH, childItems[0].getPath());
    assertEquals(TEST_ITEM_NAME, childItems[0].getName());
    assertFalse(childItems[0].isDirectory());
  }
}
