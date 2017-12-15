package com.paxata.connector.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.paxata.connector.spi.ColumnPath;
import com.paxata.connector.spi.DataSourceRecordIterator;
import com.paxata.connector.spi.item.RecordItemSchema;
import com.paxata.connector.spi.log.ImportMessages;
import com.paxata.connector.spi.value.BinaryValue;
import com.paxata.connector.spi.value.BooleanValue;
import com.paxata.connector.spi.value.ItemSchema;
import com.paxata.connector.spi.value.ItemValue;
import com.paxata.connector.spi.value.NumberValue;
import com.paxata.connector.spi.value.RecordValue;
import com.paxata.connector.spi.value.TextValue;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * AWS DynamoDB {@link DataSourceRecordIterator data source record iterator} implementation.
 */
public class DynamoDBDataSourceRecordIterator
  implements DataSourceRecordIterator {

  private ColumnPath[] columns;
  private Iterator<Item> iterator;

  DynamoDBDataSourceRecordIterator(RecordItemSchema recordSchema, String tableName,
                                   long start, long limit, ColumnPath[] columns,
                                   DynamoDB dynamoDB) {
    this.columns = columns;

    // include all columns, if nothing is specified
    if (columns == null) {
      final List<ColumnPath> allColumns = new ArrayList<>();

      for (ItemSchema field : recordSchema.getSchema().getFields()) {
        ColumnPath column = new ColumnPath();
        column.add(field.getName());
        allColumns.add(column);
      }

      this.columns = allColumns.toArray(new ColumnPath[0]);
    }

    // let us scan the table
    // and prepare the underlying iterator
    final ScanSpec spec = new ScanSpec().withMaxResultSize((int) (start + limit));
    final ItemCollection<ScanOutcome> items = dynamoDB.getTable(tableName).scan(spec);

    iterator = items.iterator();
    for (int startIndex = 0; startIndex < start && iterator.hasNext(); startIndex++) {
      iterator.next();
    }
  }

  @Override
  public RecordValue next() {
    final List<ItemValue> itemValues = new ArrayList<>();
    final ImportMessages logItems = new ImportMessages();

    final Map<String, Object> attributes = iterator.next().asMap();
    for (ColumnPath column : columns) {
      final String name = column.get(0);
      final Object value = attributes.get(name);

      if (value == null) {
        itemValues.add(null);
      } else if (value instanceof String) {
        // add the string value
        itemValues.add(new TextValue((String) value));
      } else if (value instanceof Number) {
        // add the number v
        itemValues.add(new NumberValue(new BigDecimal(value.toString())));
      } else if (value instanceof byte[]) {
        // add the binary value
        itemValues.add(new BinaryValue((byte[]) value));
      } else if (value instanceof ByteBuffer) {
        // add the binary value
        itemValues.add(new BinaryValue(((ByteBuffer) value).array()));
      } else if (value instanceof Boolean) {
        // add the boolean value
        itemValues.add(new BooleanValue((boolean) value));
      } else {
        // everything else will not be imported
        // so, let us warn the user
        logItems.columnWarning(name, "unsupported.columnType");
      }
    }

    return new RecordValue(itemValues, logItems);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public void close() {
  }
}