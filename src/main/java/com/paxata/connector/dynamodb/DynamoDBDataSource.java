package com.paxata.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.paxata.connector.dynamodb.AttributeScanner.AttributeType;
import com.paxata.connector.dynamodb.config.OptionUtils;
import com.paxata.connector.spi.ColumnPath;
import com.paxata.connector.spi.DataSource;
import com.paxata.connector.spi.DataSourceRecordIterator;
import com.paxata.connector.spi.DataSourceRecordWriter;
import com.paxata.connector.spi.StreamItemInputStream;
import com.paxata.connector.spi.StreamItemOutputStream;
import com.paxata.connector.spi.exception.ConnectorException;
import com.paxata.connector.spi.item.DataSourceItem;
import com.paxata.connector.spi.item.DirectoryItem;
import com.paxata.connector.spi.item.ItemRef;
import com.paxata.connector.spi.item.RecordItem;
import com.paxata.connector.spi.item.RecordItemSchema;
import com.paxata.connector.spi.log.ImportMessages;
import com.paxata.connector.spi.value.GroupItemSchema;
import com.paxata.connector.spi.value.ItemSchema;
import com.paxata.connector.spi.value.PrimitiveItemSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * AWS DynamoDB {@link DataSource data source} implementation.
 * For now, this implementation supports {@link com.paxata.connector.spi.config.DataSourceSupportOption#BROWSE} and
 * {@link com.paxata.connector.spi.config.DataSourceSupportOption#IMPORT}.
 */
public class DynamoDBDataSource
  implements DataSource {

  private static final int ITEM_CACHE_SIZE = 100;
  private static final int ITEM_CACHE_DURATION = 60;

  private static final String PATH_SEPARATOR = "/";

  private static final String MIME_TYPE = "application/x.dbtable";

  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
  private static final int MAX_TEXT_SIZE = 400 * 1024; // 400 KB
  private static final int MAX_NUMBER_PRECISION = 38; // 38
  private static final int MAX_NUMBER_SCALE = 38;

  private Map<String, String> options;

  private DynamoDB dynamoDB;

  private LoadingCache<String, RecordItem> itemCache =
    CacheBuilder.newBuilder().maximumSize(ITEM_CACHE_SIZE).expireAfterAccess(ITEM_CACHE_DURATION, TimeUnit.MINUTES)
                .build(
                  new CacheLoader<String, RecordItem>() {
                    public RecordItem load(String key) {
                      return loadItem(key);
                    }
                  });

  DynamoDBDataSource(Map<String, String> options) {
    this.options = options;

    dynamoDB = DynamoDBUtils.createDynamoDB(options);
  }

  // we need this just to test the connection
  // from connector factory
  DynamoDB getDynamoDB() {
    return dynamoDB;
  }

  /**
   * @return {@code "/"}
   */
  @Override
  public String getRootPath() {
    return PATH_SEPARATOR;
  }

  /**
   * @return {@link DirectoryItem table directory}
   */
  @Override
  public DataSourceItem getRootItem() {
    return new DirectoryItem(PATH_SEPARATOR, PATH_SEPARATOR, false);
  }

  /**
   * @return {@link ItemRef table references}
   */
  @Override
  public ItemRef[] getChildItems(String directoryPath, int skip, int limit) {
    if (!PATH_SEPARATOR.equals(directoryPath)) {
      throw new ConnectorException("invalid.directoryPath");
    }

    final List<ItemRef> items = new ArrayList<>();
    int skipIndex = 0;

    final TableCollection<ListTablesResult> tables = dynamoDB.listTables();
    for (Table table : tables) {
      if (skipIndex < skip) {
        skipIndex++;
        continue;
      }

      if (limit != -1 && limit == items.size()) {
        break;
      }

      final String tableName = table.getTableName();
      items.add(new ItemRef(PATH_SEPARATOR + tableName, tableName, false));
    }

    return items.toArray(new ItemRef[0]);
  }

  /**
   * @return {@link RecordItem table item}
   */
  @Override
  public DataSourceItem getItem(String itemPath) {
    // we always look in the cache
    // as our cache will load automatically if required
    return itemCache.getUnchecked(itemPath);
  }

  private RecordItem loadItem(String absolutePath) {
    return createRecordItem(absolutePath, extractTableName(absolutePath));
  }

  private String extractTableName(String itemPath) {
    return itemPath.substring(itemPath.lastIndexOf(PATH_SEPARATOR) + 1);
  }

  private RecordItem createRecordItem(String itemPath, String tableName) {
    final List<ItemSchema> itemSchemas = new ArrayList<>();
    final ImportMessages logItems = new ImportMessages();

    final TableDescription description = dynamoDB.getTable(tableName).describe();

    final Map<String, AttributeType> attributes;
    try {
      attributes =
        DynamoDBUtils.scanAttributes(dynamoDB, tableName, Integer.valueOf(OptionUtils.getSampleItems(options)));
    } catch (Exception e) {
      throw new ConnectorException("attributes.scan.failed");
    }

    for (String name : attributes.keySet()) {
      final AttributeType type = attributes.get(name);

      switch (type) {
        case STRING:
          // add the column with string data type
          itemSchemas.add(PrimitiveItemSchema.makeText(name, false, MAX_TEXT_SIZE));
          break;
        case NUMBER:
          // add the column with number data type
          itemSchemas.add(PrimitiveItemSchema.makeNumber(name, false, MAX_NUMBER_PRECISION, MAX_NUMBER_SCALE));
          break;
        case BINARY:
          // add the column with binary data type
          itemSchemas.add(PrimitiveItemSchema.makeBinary(name, false));
          break;
        case BOOLEAN:
          // add the column with boolean data type
          itemSchemas.add(PrimitiveItemSchema.makeBoolean(name, false));
          break;
        default:
          // everything else will not be imported
          // so, let us warn the user
          logItems.columnWarning(name, "unsupported.columnType", name);
      }
    }

    final GroupItemSchema groupSchema = new GroupItemSchema("Schema", false, itemSchemas);
    final RecordItemSchema recordItemSchema = new RecordItemSchema(groupSchema, logItems);
    return new RecordItem(itemPath, tableName, MIME_TYPE, description.getItemCount(), recordItemSchema);
  }

  /**
   * Creates a {@link DynamoDBDataSourceRecordIterator DynamoDB data source record iterator} using the given options.
   */
  @Override
  public DataSourceRecordIterator openRecordIterator(String absolutePath, long start, long limit,
                                                     ColumnPath[] columns) {
    // we always look in the cache
    // as our cache will load automatically if required
    return new DynamoDBDataSourceRecordIterator(itemCache.getUnchecked(absolutePath).getSchema(),
                                                extractTableName(absolutePath), start, limit, columns, dynamoDB);
  }

  @Override
  public DataSourceRecordWriter createRecordItem(String parentDirectory, String name, GroupItemSchema schema,
                                                 String mimeType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamItemInputStream openInputStream(String absolutePath, boolean preview) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamItemOutputStream createStreamItem(String parentDirectory, String name, String encoding,
                                                 String mimeType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordItemSchema getQueryResultsSchema(String query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DataSourceRecordIterator openQueryResultsIterator(String query) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DirectoryItem createChildDirectory(String parentPath, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean move(String fromPath, String toPath) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    dynamoDB.shutdown();
  }
}