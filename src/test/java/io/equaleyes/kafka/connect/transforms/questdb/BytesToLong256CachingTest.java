package io.equaleyes.kafka.connect.transforms.questdb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for the caching functionality in BytesToLong256 transformer.
 */
public class BytesToLong256CachingTest {

    private final BytesToLong256<SourceRecord> xformValue = new BytesToLong256.Value<>();

    @AfterEach
    public void teardown() {
        xformValue.close();
    }

    @Test
    public void testCachingConfigDisabled() {
        // Configure transformer with caching disabled
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 0);
        xformValue.configure(configs);

        // Create test data
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67};
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .build();

        // Create and transform first record
        Struct value1 = new Struct(schema);
        value1.put("binary_field", testBytes);
        SourceRecord record1 = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key1", schema, value1);
        SourceRecord transformed1 = xformValue.apply(record1);

        // Create and transform identical record
        Struct value2 = new Struct(schema);
        value2.put("binary_field", Arrays.copyOf(testBytes, testBytes.length)); // Use copy of byte array
        SourceRecord record2 = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key2", schema, value2);
        SourceRecord transformed2 = xformValue.apply(record2);

        // Verify transformations
        assertEquals("0x01234567i", ((Struct) transformed1.value()).get("binary_field"));
        assertEquals("0x01234567i", ((Struct) transformed2.value()).get("binary_field"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 10, 100})
    public void testCachingWithDifferentSizes(int cacheSize) {
        // Configure transformer with specified cache size
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, cacheSize);
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .build();

        // Create and transform records with the same byte array
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67};

        // Transform multiple records with identical content
        for (int i = 0; i < 5; i++) {
            Struct value = new Struct(schema);
            value.put("binary_field", Arrays.copyOf(testBytes, testBytes.length)); // Use copy of byte array
            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    Schema.STRING_SCHEMA, "key" + i, schema, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformation
            assertEquals("0x01234567i", ((Struct) transformed.value()).get("binary_field"));
        }
    }

    @Test
    public void testCachingWithStringConversion() {
        // Configure transformer with caching
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "hex_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 10);
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("hex_field", Schema.STRING_SCHEMA)
                .build();

        // Create test data with a hex string
        String hexString = "0xabcdef";

        // Transform multiple records with the same hex string
        for (int i = 0; i < 3; i++) {
            Struct value = new Struct(schema);
            value.put("hex_field", hexString);
            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    Schema.STRING_SCHEMA, "key" + i, schema, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformation
            assertEquals("0xABCDEFi", ((Struct) transformed.value()).get("hex_field"));
        }
    }

    @Test
    public void testCachingWithBase64String() {
        // Configure transformer with caching
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "base64_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 10);
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("base64_field", Schema.STRING_SCHEMA)
                .build();

        // Create test data with a Base64 encoded string
        byte[] bytes = new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        String base64String = Base64.getEncoder().encodeToString(bytes);

        // Transform multiple records with the same Base64 string
        for (int i = 0; i < 3; i++) {
            Struct value = new Struct(schema);
            value.put("base64_field", base64String);
            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    Schema.STRING_SCHEMA, "key" + i, schema, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformation
            assertEquals("0x0123456789ABCDEFi", ((Struct) transformed.value()).get("base64_field"));
        }
    }

    @Test
    public void testCachingWithMultipleFields() {
        // Configure transformer with multiple fields
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field,hex_field,base64_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 10);
        xformValue.configure(configs);

        // Create test data
        byte[] bytes1 = new byte[]{0x01, 0x23, 0x45, 0x67};
        String hexString = "0xabcdef";
        byte[] bytes2 = new byte[]{(byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        String base64String = Base64.getEncoder().encodeToString(bytes2);

        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .field("hex_field", Schema.STRING_SCHEMA)
                .field("base64_field", Schema.STRING_SCHEMA)
                .build();

        // Process multiple records with the same data
        for (int i = 0; i < 3; i++) {
            Struct value = new Struct(schema);
            value.put("binary_field", Arrays.copyOf(bytes1, bytes1.length));
            value.put("hex_field", hexString);
            value.put("base64_field", base64String);

            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    Schema.STRING_SCHEMA, "key" + i, schema, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformations
            assertEquals("0x01234567i", ((Struct) transformed.value()).get("binary_field"));
            assertEquals("0xABCDEFi", ((Struct) transformed.value()).get("hex_field"));
            assertEquals("0x89ABCDEFi", ((Struct) transformed.value()).get("base64_field"));
        }
    }

    @Test
    public void testCacheEviction() {
        // Configure transformer with small cache size
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 2); // Very small cache size
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .build();

        // Create different byte arrays
        byte[][] bytesArray = new byte[5][];
        for (int i = 0; i < 5; i++) {
            bytesArray[i] = new byte[]{(byte) i, (byte) (i + 1), (byte) (i + 2), (byte) (i + 3)};
        }

        // First, fill the cache and overflow it
        for (int i = 0; i < 5; i++) {
            Struct value = new Struct(schema);
            value.put("binary_field", bytesArray[i]);

            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    Schema.STRING_SCHEMA, "key" + i, schema, value);
            xformValue.apply(record);
        }

        // Now re-process the first record to verify it works correctly even if it was evicted
        Struct value = new Struct(schema);
        value.put("binary_field", bytesArray[0]);

        SourceRecord record = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key0", schema, value);
        SourceRecord transformed = xformValue.apply(record);

        // Verify the transformation still works correctly
        String expectedHex = String.format("0x%02X%02X%02X%02Xi",
                bytesArray[0][0] & 0xFF,
                bytesArray[0][1] & 0xFF,
                bytesArray[0][2] & 0xFF,
                bytesArray[0][3] & 0xFF);

        assertEquals(expectedHex, ((Struct) transformed.value()).get("binary_field"));
    }

    @Test
    public void testNormalizationCaching() {
        // Configure transformer with caching
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "hex_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 10);
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("hex_field", Schema.STRING_SCHEMA)
                .build();

        // Create test data with variations of the same hex string
        String[] hexVariations = {
                "0xabcdef",
                "0xABCDEF",
                "0xabcdefi",
                "0xABCDEFi"
        };

        // Expected normalized result
        String expected = "0xABCDEFi";

        // Transform records with variations of the same hex string
        for (String hexString : hexVariations) {
            Struct value = new Struct(schema);
            value.put("hex_field", hexString);
            SourceRecord record = new SourceRecord(null, null, "topic", 0,
                    Schema.STRING_SCHEMA, "key", schema, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformation normalizes to the same result
            assertEquals(expected, ((Struct) transformed.value()).get("hex_field"));
        }
    }

    @Test
    public void testEmptyByteArray() {
        // Configure transformer with caching
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 10);
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .build();

        // Transform an empty byte array multiple times
        for (int i = 0; i < 3; i++) {
            Struct value = new Struct(schema);
            value.put("binary_field", new byte[0]);

            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    Schema.STRING_SCHEMA, "key" + i, schema, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformation for empty array
            assertEquals("0x08000000000000000800000000000000080000000000000008000000000000000i",
                    ((Struct) transformed.value()).get("binary_field"));
        }
    }

    @Test
    public void testCachingWithSchemaless() {
        // Configure transformer with caching
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field,hex_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 10);
        xformValue.configure(configs);

        // Create test data
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67};
        String hexString = "0xabcdef";

        // Process multiple schemaless records with the same data
        for (int i = 0; i < 3; i++) {
            Map<String, Object> value = new HashMap<>();
            value.put("binary_field", Arrays.copyOf(testBytes, testBytes.length));
            value.put("hex_field", hexString);
            value.put("other_field", "test" + i);

            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    null, "key" + i, null, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformation
            @SuppressWarnings("unchecked")
            Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
            assertEquals("0x01234567i", transformedValue.get("binary_field"));
            assertEquals("0xABCDEFi", transformedValue.get("hex_field"));
            assertEquals("test" + i, transformedValue.get("other_field"));
        }
    }

    @Test
    public void testCachingAfterClose() {
        // Configure transformer with caching
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field");
        configs.put(BytesToLong256.CONVERSION_CACHE_SIZE_CONFIG, 10);
        xformValue.configure(configs);

        // Create test data
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67};
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .build();

        // Create and transform a record
        Struct value1 = new Struct(schema);
        value1.put("binary_field", testBytes);
        SourceRecord record1 = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key1", schema, value1);
        SourceRecord transformed1 = xformValue.apply(record1);

        assertEquals("0x01234567i", ((Struct) transformed1.value()).get("binary_field"));

        // Close the transformer (should clear the cache)
        xformValue.close();

        // Re-configure and transform another identical record
        xformValue.configure(configs);

        Struct value2 = new Struct(schema);
        value2.put("binary_field", Arrays.copyOf(testBytes, testBytes.length));
        SourceRecord record2 = new SourceRecord(null, null, "topic", 1,
                Schema.STRING_SCHEMA, "key2", schema, value2);
        SourceRecord transformed2 = xformValue.apply(record2);

        // Transformation should still work after closing and re-configuring
        assertEquals("0x01234567i", ((Struct) transformed2.value()).get("binary_field"));
    }

    @Test
    public void testDefaultCacheSize() {
        // Configure transformer with default cache size (not explicitly setting it)
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "binary_field");
        xformValue.configure(configs);

        // Create test data
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67};
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .build();

        // Create and transform multiple records with identical content
        for (int i = 0; i < 3; i++) {
            Struct value = new Struct(schema);
            value.put("binary_field", Arrays.copyOf(testBytes, testBytes.length));
            SourceRecord record = new SourceRecord(null, null, "topic", i,
                    Schema.STRING_SCHEMA, "key" + i, schema, value);
            SourceRecord transformed = xformValue.apply(record);

            // Verify transformation
            assertEquals("0x01234567i", ((Struct) transformed.value()).get("binary_field"));
        }
    }
}