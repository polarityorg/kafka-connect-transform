package io.equaleyes;

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
 * Tests for the BytesToHexString transformer.
 */
public class BytesToHexStringTest {

    private final BytesToHexString<SourceRecord> xformKey = new BytesToHexString.Key<>();
    private final BytesToHexString<SourceRecord> xformValue = new BytesToHexString.Value<>();

    public static Stream<Arguments> nullWithDefaultData() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, "0x0A0B0C")
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
                () -> xformKey.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "")));
    }

    @Test
    public void testConfigEmptyFields() {
        assertThrows(ConfigException.class,
                () -> xformKey.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, ",")));
    }

    @Test
    public void testTransformNullRecord() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", Schema.STRING_SCHEMA, null);
        SourceRecord transformed = xformValue.apply(original);
        assertEquals(original, transformed);
    }

    @ParameterizedTest
    @MethodSource("nullWithDefaultData")
    public void testFieldWithDefaultValueRecordWithSchema(boolean replaceNullWithDefault, Object expectedValue) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(BytesToHexString.FIELDS_CONFIG, "binary_field");
        configs.put(BytesToHexString.REPLACE_NULL_WITH_DEFAULT_CONFIG, replaceNullWithDefault);
        xformValue.configure(configs);

        // Create schema with default value for binary field
        byte[] defaultBytes = new byte[]{0x0A, 0x0B, 0x0C};
        Schema structSchema = SchemaBuilder.struct()
                .field("binary_field", SchemaBuilder.bytes().optional().defaultValue(defaultBytes).build())
                .build();

        // Create record with null binary field
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", structSchema,
                new Struct(structSchema).put("binary_field", null));

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Check schema type changed to string
        assertEquals(Schema.Type.STRING, transformed.valueSchema().field("binary_field").schema().type());

        // Check value replacement behavior based on setting
        assertEquals(expectedValue, ((Struct) transformed.value()).getWithoutDefault("binary_field"));
    }

    @Test
    public void testNullKeyRecordWithSchema() {
        xformKey.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void testNullKeyRecordSchemaless() {
        xformKey.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, null, Schema.STRING_SCHEMA, "value");
        SourceRecord transformed = xformKey.apply(original);
        assertEquals(original, transformed);
    }

    @Test
    public void testConvertByteArrayWithSchema() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};

        // Create schema for binary data
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with binary field
        Struct value = new Struct(schema);
        value.put("binary_field", testBytes);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify schema transformation
        Schema transformedSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, transformedSchema.field("binary_field").schema().type());
        assertEquals(Schema.Type.STRING, transformedSchema.field("other_field").schema().type());

        // Verify value transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals("0x0123456789ABCDEF", transformedValue.get("binary_field"));
        assertEquals("test", transformedValue.get("other_field"));
    }

    @Test
    public void testConvertByteBufferWithSchema() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        ByteBuffer byteBuffer = ByteBuffer.wrap(testBytes);

        // Create schema for binary data
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with binary field
        Struct value = new Struct(schema);
        value.put("binary_field", byteBuffer);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify value transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals("0x0123456789ABCDEF", transformedValue.get("binary_field"));
    }

    @Test
    public void testConvertBase64StringWithSchema() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        String base64String = Base64.getEncoder().encodeToString(testBytes);

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.STRING_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with base64 encoded string
        Struct value = new Struct(schema);
        value.put("binary_field", base64String);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals("0x0123456789ABCDEF", transformedValue.get("binary_field"));
    }

    @Test
    public void testConvertMultipleFieldsWithSchema() {
        xformValue.configure(Collections.singletonMap(
                BytesToHexString.FIELDS_CONFIG, "binary_field1,binary_field2"));

        byte[] testBytes1 = new byte[]{0x01, 0x23, 0x45, 0x67};
        byte[] testBytes2 = new byte[]{(byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field1", Schema.BYTES_SCHEMA)
                .field("binary_field2", Schema.BYTES_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create record
        Struct value = new Struct(schema);
        value.put("binary_field1", testBytes1);
        value.put("binary_field2", testBytes2);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformation
        Struct transformedValue = (Struct) transformed.value();
        assertEquals("0x01234567", transformedValue.get("binary_field1"));
        assertEquals("0x89ABCDEF", transformedValue.get("binary_field2"));
        assertEquals("test", transformedValue.get("other_field"));
    }

    @Test
    public void testConvertByteArraySchemaless() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};

        // Create record with binary field
        Map<String, Object> value = new HashMap<>();
        value.put("binary_field", testBytes);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, "key", null, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify value transformation
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        assertEquals("0x0123456789ABCDEF", transformedValue.get("binary_field"));
        assertEquals("test", transformedValue.get("other_field"));
    }

    @Test
    public void testConvertBase64StringSchemaless() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        String base64String = Base64.getEncoder().encodeToString(testBytes);

        // Create record with base64 encoded string
        Map<String, Object> value = new HashMap<>();
        value.put("binary_field", base64String);
        value.put("other_field", "test");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                null, "key", null, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformation
        @SuppressWarnings("unchecked")
        Map<String, Object> transformedValue = (Map<String, Object>) transformed.value();
        assertEquals("0x0123456789ABCDEF", transformedValue.get("binary_field"));
    }

    @Test
    public void testCacheReusesSchema() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        byte[] testBytes = new byte[]{0x01, 0x23, 0x45, 0x67};

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .field("other_field", Schema.STRING_SCHEMA)
                .build();

        // Create first record
        Struct value1 = new Struct(schema);
        value1.put("binary_field", testBytes);
        value1.put("other_field", "test1");

        SourceRecord record1 = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key1", schema, value1);

        // Create second record with same schema but different values
        Struct value2 = new Struct(schema);
        value2.put("binary_field", new byte[]{(byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF});
        value2.put("other_field", "test2");

        SourceRecord record2 = new SourceRecord(null, null, "topic", 1,
                Schema.STRING_SCHEMA, "key2", schema, value2);

        // Transform both records
        SourceRecord transformed1 = xformValue.apply(record1);
        SourceRecord transformed2 = xformValue.apply(record2);

        // Verify transformed schemas are the same object (cached)
        assertSame(transformed1.valueSchema(), transformed2.valueSchema());

        // Verify values were correctly transformed
        assertEquals("0x01234567", ((Struct) transformed1.value()).get("binary_field"));
        assertEquals("0x89ABCDEF", ((Struct) transformed2.value()).get("binary_field"));
    }

    @Test
    public void testEmptyByteArray() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.BYTES_SCHEMA)
                .build();

        // Create record with empty byte array
        Struct value = new Struct(schema);
        value.put("binary_field", new byte[0]);

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify empty array produces empty string
        assertEquals("", ((Struct) transformed.value()).get("binary_field"));
    }

    @Test
    public void testDebeziumCdcFormat() {
        // Test with data similar to what would come from Debezium
        xformValue.configure(Collections.singletonMap(
                BytesToHexString.FIELDS_CONFIG, "verifying_pubkey,owner_identity_pubkey,raw_tx"));

        // Sample Base64 encoded data similar to what would come from Debezium
        String base64VerifyingPubkey = "A07vWKR6tcnHVKQtyjizyRqTZ+ynKbvHpE9KyAf92c1x";
        String base64OwnerPubkey = "A8cOjmAwrO89XMOHekweq9ycsBDzFwfvQd8il58tXFDb";
        String base64RawTx = "AgAAAAABAaEY4AYpCYdqJXwYzIhecxeSiiVCEKfCDHK2qSCDkrtQAAAAAADQBwBAAgEAAAAAAAAAIlEgAqHYMEwTF+dVWJaMX8leU9tdGL5obv+VeHkDovDhPYUAAAAAAAAAAAIBUQFAdCha56sr+roclfG/SGi/bNC80j4bx7O4dbDZ3rNplTf7Hz0oREPoV71iXto4KK88CGnSZOYh1Dra0oZ98GQyUwAAAAA=";

        // Create schema that resembles a Debezium CDC record after unwrapping
        Schema cdcSchema = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("create_time", Schema.STRING_SCHEMA)
                .field("value", Schema.INT32_SCHEMA)
                .field("status", Schema.STRING_SCHEMA)
                .field("verifying_pubkey", Schema.BYTES_SCHEMA)
                .field("owner_identity_pubkey", Schema.BYTES_SCHEMA)
                .field("raw_tx", Schema.BYTES_SCHEMA)
                .build();

        // Create record
        Struct cdcValue = new Struct(cdcSchema);
        cdcValue.put("id", "01958e31-21ff-767b-b752-a3e712c34c47");
        cdcValue.put("create_time", "2025-03-13T06:29:02.570959Z");
        cdcValue.put("value", 1);
        cdcValue.put("status", "AVAILABLE");
        cdcValue.put("verifying_pubkey", Base64.getDecoder().decode(base64VerifyingPubkey));
        cdcValue.put("owner_identity_pubkey", Base64.getDecoder().decode(base64OwnerPubkey));
        cdcValue.put("raw_tx", Base64.getDecoder().decode(base64RawTx));

        SourceRecord record = new SourceRecord(null, null, "spark.public.tree_nodes", 0,
                null, null, cdcSchema, cdcValue);

        // Apply transformation
        SourceRecord transformed = xformValue.apply(record);

        // Verify result
        Struct result = (Struct) transformed.value();

        // The expected hex values should match the Base64 decoded and hex encoded values
        assertEquals("0x034EEF58A47AB5C9C754A42DCA38B3C91A9367ECA729BBC7A44F4AC807FDD9CD71",
                result.get("verifying_pubkey"));
        assertEquals("0x03C70E8E6030ACEF3D5CC3877A4C1EABDC9CB010F31707EF41DF22979F2D5C50DB",
                result.get("owner_identity_pubkey"));
        assertEquals("0x02000000000101A118E0062909876A257C18CC885E7317928A254210A7C20C72B6A9208392BB500000000000D007004002010000000000000022512002A1D8304C1317E75558968C5FC95E53DB5D18BE686EFF95787903A2F0E13D850000000000000000020151014074285AE7AB2BFABA1C95F1BF4868BF6CD0BCD23E1BC7B3B875B0D9DEB3699537FB1F3D284443E857BD625EDA3828AF3C0869D264E621D43ADAD2867DF064325300000000",
                result.get("raw_tx"));

        // Non-binary fields should be unchanged
        assertEquals("01958e31-21ff-767b-b752-a3e712c34c47", result.get("id"));
        assertEquals("AVAILABLE", result.get("status"));

        // Schema should be updated
        Schema transformedSchema = transformed.valueSchema();
        assertEquals(Schema.Type.STRING, transformedSchema.field("verifying_pubkey").schema().type());
        assertEquals(Schema.Type.STRING, transformedSchema.field("owner_identity_pubkey").schema().type());
        assertEquals(Schema.Type.STRING, transformedSchema.field("raw_tx").schema().type());
    }

    @Test
    public void testInvalidBase64Handling() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));

        // Create schema
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", Schema.STRING_SCHEMA)
                .build();

        // Create record with invalid Base64 string
        Struct value = new Struct(schema);
        value.put("binary_field", "not base64!");

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify invalid Base64 is kept as-is
        assertEquals("not base64!", ((Struct) transformed.value()).get("binary_field"));
    }

    @Test
    public void testSchemaDefaultValueConversion() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));
        byte[] defaultBytes = new byte[]{0x01, 0x23, 0x45, 0x67};

        // Create schema with default value
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", SchemaBuilder.bytes().defaultValue(defaultBytes).build())
                .build();

        // Create record
        Struct value = new Struct(schema);
        value.put("binary_field", new byte[]{(byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF});

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify default value was transformed
        assertEquals("0x01234567", transformed.valueSchema().field("binary_field").schema().defaultValue());

        // Verify actual value was transformed
        assertEquals("0x89ABCDEF", ((Struct) transformed.value()).get("binary_field"));
    }

    @Test
    public void testOptionalFieldHandling() {
        xformValue.configure(Collections.singletonMap(BytesToHexString.FIELDS_CONFIG, "binary_field"));

        // Create schema with optional field
        Schema schema = SchemaBuilder.struct()
                .field("binary_field", SchemaBuilder.bytes().optional().build())
                .build();

        // Create record with null value
        Struct value = new Struct(schema);
        value.put("binary_field", null);

        SourceRecord original = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "key", schema, value);

        // Transform the record
        SourceRecord transformed = xformValue.apply(original);

        // Verify transformed field is optional
        assertTrue(transformed.valueSchema().field("binary_field").schema().isOptional());

        // Verify null value remains null
        assertNull(((Struct) transformed.value()).get("binary_field"));
    }

    @Test
    public void testVersion() {
        // Make sure version is consistent
        assertEquals("1.0.0", xformKey.version());
        assertEquals("1.0.0", xformValue.version());
    }
}