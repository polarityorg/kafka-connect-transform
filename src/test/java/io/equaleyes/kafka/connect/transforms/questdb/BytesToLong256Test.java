package io.equaleyes.kafka.connect.transforms.questdb;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the BytesToLong256 transformer.
 */
public class BytesToLong256Test {

    private final BytesToLong256<SourceRecord> xformKey = new BytesToLong256.Key<>();
    private final BytesToLong256<SourceRecord> xformValue = new BytesToLong256.Value<>();

    public static Stream<Arguments> hexToLong256Data() {
        return Stream.of(
                Arguments.of("0x00000000000000000000000000000311", "0x311i"),
                Arguments.of("0x000000000000000000000000000003D4", "0x3d4i"),
                Arguments.of("0x000000000000000000000000000003E3", "0x3e3i"),
                Arguments.of("0x00000000000000000000000000030188", "0x30188i"),
                Arguments.of("0x00000000000000000000000000000014", "0x14i"),
                Arguments.of("0x00000000000000000000000000000005", "0x5i"),
                Arguments.of("0x259DA6542D4362000000000000000000", "0x259da6542d4362000000000000000000i"),
                Arguments.of("0x000000000000000000000000000003E8", "0x3e8i"),
                Arguments.of("0x000000000000000000000000000000C8", "0xc8i"),
                Arguments.of("0x0000000000000000000000000000030C", "0x30ci"),
                Arguments.of("0x000000000000000000000000000022B8", "0x22b8i"),
                Arguments.of("0x00000000000000000000000000000064", "0x64i"),
                Arguments.of("0x00000000000000000000000000030958", "0x30958i"),
                Arguments.of("0x000000000000000000000000000F4240", "0xf4240i"),
                Arguments.of("0x00000000000000000000000000030570", "0x30570i"),
                Arguments.of("0x0000000000000000000000000002FDA0", "0x2fda0i"),
                Arguments.of("0x000000000000000000000000000026AC", "0x26aci"),
                Arguments.of("0x00000000000000000000000005F5E100", "0x5f5e100i"),
                Arguments.of("0x0000000000000000000000000000263E", "0x263ei"),
                Arguments.of("0x00000000000000000000000000000FA0", "0xfa0i"),
                Arguments.of("0x0000000000000000000000000000000A", "0xai"),
                Arguments.of("0x000000000000000000000000000186A0", "0x186a0i"),
                Arguments.of("0x000000000000000000000000000003DE", "0x3dei"),
                Arguments.of("0x00000000000000000000000000002710", "0x2710i"),
                Arguments.of("0x00000000000000000000000000000316", "0x316i"),
                Arguments.of("0x000000000000000000000000000026A2", "0x26a2i"),
                Arguments.of("0x0000000000000000000000000002F9B8", "0x2f9b8i"),
                Arguments.of("0x00000000000000000000000000030D40", "0x30d40i")
        );
    }

    public static Stream<Arguments> byteArrayToLong256Data() {
        return Stream.of(
                Arguments.of(new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x0D, 0x40}, "0x30d40i"), // 200000
                Arguments.of(new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, (byte) 0xE8}, "0x3e8i"),   // 1000
                Arguments.of(new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A}, "0x2ai")           // 42
        );
    }

    public static Stream<Arguments> nullWithDefaultData() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, "0x2ai") // 42 in hex with suffix
        );
    }

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testConfigEmpty() {
        assertThrows(ConfigException.class,
                () -> xformKey.configure(Collections.singletonMap(BytesToLong256.FIELDS_CONFIG, "")));
    }

    @Test
    public void testConfigEmptyFields() {
        assertThrows(ConfigException.class,
                () -> xformKey.configure(Collections.singletonMap(BytesToLong256.FIELDS_CONFIG, ",")));
    }

    @Test
    public void testTransformNullRecord() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @ParameterizedTest
    @MethodSource("nullWithDefaultData")
    public void testFieldWithDefaultValueRecordWithSchema(boolean replaceNullWithDefault, Object expectedValue) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        configs.put(BytesToLong256.REPLACE_NULL_WITH_DEFAULT_CONFIG, replaceNullWithDefault);
        xformValue.configure(configs);

        // Create schema with default value for binary field
        byte[] defaultBytes = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A}; // 42 in hex
        Schema structSchema = SchemaBuilder.struct()
                .field("token_amount", SchemaBuilder.bytes().optional().defaultValue(defaultBytes).build())
                .build();

        // Create record with null binary field
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", structSchema,
                new Struct(structSchema).put("token_amount", null));

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Check schema type changed to string (for long256)
        assertEquals(Schema.Type.STRING, transformed.valueSchema().field("token_amount").schema().type());

        // Check value replacement behavior based on setting
        assertEquals(expectedValue, ((Struct) transformed.value()).getWithoutDefault("token_amount"));
    }

    @Test
    public void testNullKeyRecordWithSchema() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void testNullKeyRecordSchemaless() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @ParameterizedTest
    @MethodSource("hexToLong256Data")
    public void testConvertHexStringToLong256WithSchema(String hexString, String expectedValue) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", Schema.STRING_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with hex string field
        Struct value = new Struct(schema);
        value.put("token_amount", hexString);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify schema transformation
        Schema transformedSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, transformedSchema.field("token_amount").schema().type());
        assertEquals(Schema.Type.STRING, transformedSchema.field("other_field").schema().type());

        // Verify value transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals(expectedValue, transformedValue.get("token_amount"));
        assertEquals("test", transformedValue.get("other_field"));
    }

    @ParameterizedTest
    @MethodSource("byteArrayToLong256Data")
    public void testConvertByteArrayWithSchema(byte[] bytes, String expectedValue) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // Create schema for binary data
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", Schema.BYTES_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with binary field
        Struct value = new Struct(schema);
        value.put("token_amount", bytes);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify schema transformation
        Schema transformedSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, transformedSchema.field("token_amount").schema().type());
        assertEquals(Schema.Type.STRING, transformedSchema.field("other_field").schema().type());

        // Verify value transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals(expectedValue, transformedValue.get("token_amount"));
        assertEquals("test", transformedValue.get("other_field"));
    }

    @Test
    public void testConvertByteBufferWithSchema() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // 0x00000000000003E8 = 1000 in decimal
        byte[] testBytes = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, (byte) 0xE8};
        ByteBuffer byteBuffer = ByteBuffer.wrap(testBytes);

        // Create schema for binary data
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", Schema.BYTES_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with binary field
        Struct value = new Struct(schema);
        value.put("token_amount", byteBuffer);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify value transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals("0x3e8i", transformedValue.get("token_amount"));
    }

    @Test
    public void testConvertBase64StringWithSchema() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // 0x000000000000030C = 780 in decimal
        byte[] testBytes = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x0C};
        String base64String = Base64.getEncoder().encodeToString(testBytes);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", Schema.STRING_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with base64 encoded string
        Struct value = new Struct(schema);
        value.put("token_amount", base64String);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals("0x30ci", transformedValue.get("token_amount"));
    }

    @Test
    public void testConvertMultipleFieldsWithSchema() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount1,token_amount2");
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("token_amount1", Schema.STRING_SCHEMA)
                .field("token_amount2", Schema.STRING_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record
        Struct value = new Struct(schema);
        value.put("token_amount1", "0x00000000000000000000000000000064"); // 100
        value.put("token_amount2", "0x00000000000000000000000000000FA0"); // 4000
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals("0x64i", transformedValue.get("token_amount1"));
        assertEquals("0xfa0i", transformedValue.get("token_amount2"));
        assertEquals("test", transformedValue.get("other_field"));
    }

    @ParameterizedTest
    @MethodSource("hexToLong256Data")
    public void testConvertHexStringToLong256Schemaless(String hexString, String expectedValue) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // Create record with hex string field
        Map<String, Object> value = new HashMap<>();
        value.put("token_amount", hexString);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, "key", null, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify value transformation
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        assertEquals(expectedValue, transformedValue.get("token_amount"));
        assertEquals("test", transformedValue.get("other_field"));
    }

    @Test
    public void testConvertBase64StringSchemaless() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // 0x0000000000002710 = 10000 in decimal
        byte[] testBytes = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x10};
        String base64String = Base64.getEncoder().encodeToString(testBytes);

        // Create record with base64 encoded string
        Map<String, Object> value = new HashMap<>();
        value.put("token_amount", base64String);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, "key", null, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformation
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        assertEquals("0x2710i", transformedValue.get("token_amount"));
    }

    @Test
    public void testCacheReusesSchema() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", Schema.STRING_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create first record
        Struct value1 = new Struct(schema);
        value1.put("token_amount", "0x000000000000000000000000000003DE"); // 990
        value1.put("other_field", "test1");

        SourceRecord record1 = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key1", schema, value1);

        // Create second record with same schema but different values
        Struct value2 = new Struct(schema);
        value2.put("token_amount", "0x000000000000000000000000000026A2"); // 9890
        value2.put("other_field", "test2");

        SourceRecord record2 = new SourceRecord(null, null, "topic", 1,
                Schema.STRING_SCHEMA, "key2", schema, value2);

        // Transform both records
        SourceRecord transformed1 = xformValue.apply(record1);
        SourceRecord transformed2 = xformValue.apply(record2);

        // Verify transformed schemas are the same object (cached)
        assertSame(transformed1.valueSchema(), transformed2.valueSchema());

        // Verify values were correctly transformed
        assertEquals("0x3dei", ((Struct) transformed1.value()).get("token_amount"));
        assertEquals("0x26a2i", ((Struct) transformed2.value()).get("token_amount"));
    }

    // Just the modified test method for empty byte array
    @Test
    public void testEmptyByteArray() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // Create schema with OPTIONAL field to handle null values
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", SchemaBuilder.bytes().optional().build())
                .build();

        // Create record with empty byte array
        Struct value = new Struct(schema);
        value.put("token_amount", new byte[0]);

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify empty array produces null
        assertEquals("0x8000000000000000800000000000000080000000000000008000000000000000i", ((Struct) transformed.value()).get("token_amount"));
    }

    @Test
    public void testSchemaDefaultValueConversion() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // 0x00000000000000C8 = 200 in decimal
        byte[] defaultBytes = new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0xC8};

        // Create schema with default value
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", SchemaBuilder.bytes().defaultValue(defaultBytes).build())
                .build();

        // Create record
        Struct value = new Struct(schema);
        value.put("token_amount", new byte[]{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, (byte) 0xB8}); // 8888

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify default value was transformed
        assertEquals("0xc8i", transformed.valueSchema().field("token_amount").schema().defaultValue());

        // Verify actual value was transformed
        assertEquals("0x22b8i", ((Struct) transformed.value()).get("token_amount"));
    }

    @Test
    public void testOptionalFieldHandling() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToLong256.FIELDS_CONFIG, "token_amount");
        xformValue.configure(configs);

        // Create schema with optional field
        Schema schema = SchemaBuilder.struct()
                .field("token_amount", SchemaBuilder.bytes().optional().build())
                .build();

        // Create record with null value
        Struct value = new Struct(schema);
        value.put("token_amount", null);

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformed field is optional
        assertTrue(transformed.valueSchema().field("token_amount").schema().isOptional());

        // Verify null value remains null
        assertNull(((Struct) transformed.value()).get("token_amount"));
    }

    @Test
    public void testVersion() {
        // Make sure version is consistent
        assertEquals("1.0.0", xformKey.version());
        assertEquals("1.0.0", xformValue.version());
    }
}