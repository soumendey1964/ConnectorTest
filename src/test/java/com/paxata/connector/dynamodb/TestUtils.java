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

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

class TestUtils {

  static final String TEST_PROXY_SETUP = "true";
  static final String TEST_PROXY_HOST = "testProxyHost";
  static final String TEST_PROXY_PORT = "80";
  static final String TEST_PROXY_USER = "testUser";
  static final String TEST_PROXY_AUTH = "true";
  static final String TEST_PROXY_PASSWORD = "testPassword";
  static final String TEST_AWS_REGION = "us-east-1";
  static final String TEST_AWS_ACCESS_KEY = "testAccessKey";
  static final String TEST_AWS_SECRET_KEY = "testSecretKey";
  static final String TEST_SAMPLE_ITEMS = "100";

  static final String PATH_SEPARATOR = "/";
  static final String TEST_TABLE_NAME = "Music";
  static final String TEST_ITEM_NAME = TEST_TABLE_NAME;
  static final String TEST_ITEM_PATH = PATH_SEPARATOR + TEST_ITEM_NAME;

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
  }

  private TestUtils() {
  }

  static DynamoDB createDynamoDB() {
    return DynamoDBHolder.DYNAMO_DB;
  }

  static void createMusicTable(DynamoDB dynamoDB)
    throws InterruptedException {
    final ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
    keySchema.add(new KeySchemaElement().withAttributeName("Artist").withKeyType(KeyType.HASH));
    keySchema.add(new KeySchemaElement().withAttributeName("SongTitle").withKeyType(KeyType.RANGE));

    final ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("Artist").withAttributeType("S"));
    attributeDefinitions.add(new AttributeDefinition().withAttributeName("SongTitle").withAttributeType("S"));

    final ProvisionedThroughput provisionedThroughput =
      new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(10L);

    dynamoDB.createTable(new CreateTableRequest().withTableName("Music").withAttributeDefinitions(attributeDefinitions)
                                                 .withKeySchema(keySchema)
                                                 .withProvisionedThroughput(provisionedThroughput)).waitForActive();
  }

  static void createMusicItems(DynamoDB dynamoDB) {
    final Table music = dynamoDB.getTable(TEST_TABLE_NAME);

    music.putItem(new Item().withPrimaryKey("Artist", "Jonathan Coulton", "SongTitle", "Code Monkey")
                            .withString("Album", "Thing a Week Three")
                            .withString("Genre", "POP")
                            .withNumber("Track", 3)
                            .withBoolean("Favorite", true));

    music.putItem(new Item().withPrimaryKey("Artist", "Jonathan Coulton", "SongTitle", "Tom Cruise Crazy")
                            .withString("Album", "Thing a Week Three")
                            .withString("Genre", "POP")
                            .withNumber("Track", 7)
                            .withStringSet("Reviews1", new HashSet<>(Collections.singletonList("Wow"))));

    music.putItem(new Item().withPrimaryKey("Artist", "Pink Floyd", "SongTitle", "Another Brick In The Wall, Pt. 1")
                            .withString("Album", "The Wall")
                            .withString("Genre", "Rock")
                            .withNumber("Track", 3)
                            .withString("Composer", "Roger Waters")
                            .withStringSet("Reviews2", new HashSet<>(Collections.singletonList("Wow"))));

    music.putItem(new Item().withPrimaryKey("Artist", "Pink Floyd", "SongTitle", "Another Brick In The Wall, Pt. 2")
                            .withString("Album", "The Wall")
                            .withString("Genre", "Rock")
                            .withNumber("Track", 5)
                            .withString("Composer", "Roger Waters")
                            .withStringSet("Reviews3", new HashSet<>(Collections.singletonList("Wow"))));

    music.putItem(new Item().withPrimaryKey("Artist", "Harry Belafonte", "SongTitle", "Have Naguila")
                            .withString("Album", "46 Essential Caribbean Hits By Harry Belafonte")
                            .withString("Genre", "POP")
                            .withNumber("Track", 5)
                            .withBoolean("Favorite", true)
                            .withBinary("CODEC1", new byte[]{1}));

    music.putItem(new Item().withPrimaryKey("Artist", "Harry Belafonte", "SongTitle", "Day-O (Banana Boat)")
                            .withString("Album", "Very Best Of Harry Belafonte")
                            .withString("Genre", "POP")
                            .withNumber("Track", 3)
                            .withBinary("CODEC2", ByteBuffer.wrap(new byte[]{2})));
  }

  static void deleteMusicTable(DynamoDB dynamoDB)
    throws InterruptedException {
    final Table table = dynamoDB.getTable(TEST_TABLE_NAME);
    table.delete();
    table.waitForDelete();
  }

  private static class DynamoDBHolder {

    private static final DynamoDB DYNAMO_DB = new DynamoDBWrapper(DynamoDBEmbedded.create().amazonDynamoDB());
  }
}
