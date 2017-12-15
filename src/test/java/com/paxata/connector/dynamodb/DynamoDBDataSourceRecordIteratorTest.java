package com.paxata.connector.dynamodb;

import static com.paxata.connector.dynamodb.TestUtils.TEST_ITEM_PATH;
import static com.paxata.connector.dynamodb.TestUtils.TEST_OPTIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.paxata.connector.dynamodb.AttributeScanner.AttributeType;
import com.paxata.connector.spi.ColumnPath;
import com.paxata.connector.spi.DataSourceRecordIterator;
import com.paxata.connector.spi.config.SessionInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class DynamoDBDataSourceRecordIteratorTest {

  private static DynamoDBDataSource dataSource;

  @Rule
  public final ExpectedException exception = ExpectedException.none();

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

  /**
   * We should get 6 records as 'Music' table has total 6 records.
   */
  @Test
  public void testItems() {
    assertItems(dataSource.openRecordIterator(TEST_ITEM_PATH, 0, 101, null), 6);
  }

  /**
   * We should get 5 records as 'Music' table has total 6 records but we are starting from 2nd.
   */
  @Test
  public void testItemsWithStart() {
    assertItems(dataSource.openRecordIterator(TEST_ITEM_PATH, 1, 101, null), 5);
  }

  /**
   * We should get 5 records as 'Music' table has total 6 records but we have set the item count to 5.
   */
  @Test
  public void testItemsWithLimit() {
    assertItems(dataSource.openRecordIterator(TEST_ITEM_PATH, 0, 5, null), 5);
  }

  /**
   * We should get 5 records as 'Music' table has total 6 records but we are starting from 2nd
   * and have set the item count to 5.
   */
  @Test
  public void testItemsWithStartAndLimit() {
    assertItems(dataSource.openRecordIterator(TEST_ITEM_PATH, 1, 5, null), 5);
  }

  /**
   * We should expect all 6 columns, as 'Music' table has 6 column with simple type.
   */
  @Test
  public void testAllColumns() {
    // Artist, SongTitle, Album, Genre, Track, Composer

    DataSourceRecordIterator recordIterator = dataSource.openRecordIterator(TEST_ITEM_PATH, 0, 101, null);
    while (recordIterator.hasNext()) {
      assertEquals(9, recordIterator.next().getValues().size());
    }
  }

  /**
   * We should expect only 2 columns 'Artist' and 'SongTitle', even though 'Music' table has 6 column with simple type,
   * but we have specifically asked for only those 2 columns.
   */
  @Test
  public void testSomeColumns() {
    // Artist, SongTitle
    List<ColumnPath> columns = new ArrayList<>();

    ColumnPath column = new ColumnPath();
    column.add("Artist");
    columns.add(column);

    column = new ColumnPath();
    column.add("SongTitle");
    columns.add(column);

    DataSourceRecordIterator recordIterator =
      dataSource.openRecordIterator(TEST_ITEM_PATH, 0, 101, columns.toArray(new ColumnPath[0]));

    while (recordIterator.hasNext()) {
      assertEquals(2, recordIterator.next().getValues().size());
    }
  }

  @Test
  public void testClose() {
    DataSourceRecordIterator recordIterator = dataSource.openRecordIterator(TEST_ITEM_PATH, 0, 101, null);
    recordIterator.close();
  }

  private void assertItems(DataSourceRecordIterator recordIterator, int itemCount) {
    for (int i = 0; i < itemCount; i++) {
      assertTrue(recordIterator.hasNext());
      assertNotNull(recordIterator.next());
    }

    assertFalse(recordIterator.hasNext());
    exception.expect(NoSuchElementException.class);
    recordIterator.next();
  }
}
