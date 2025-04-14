package io.equaleyes.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * A Kafka Connect SMT that converts bytes fields to hex string with '0x' prefix.
 *
 * @param <R> The record type
 */
public abstract class BytesToHexString<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String FIELDS_CONFIG = "fields";
    public static final String REPLACE_NULL_WITH_DEFAULT_CONFIG = "replace.null.with.default";
    public static final String CONVERSION_CACHE_SIZE_CONFIG = "conversion.cache.size";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.LambdaValidator.with(
                            (name, valueObject) -> {
                                @SuppressWarnings("unchecked")
                                List<String> value = (List<String>) valueObject;
                                if (value == null || value.isEmpty()) {
                                    throw new ConfigException("Must specify at least one field to convert to hex string.");
                                }
                                // Validate each field name is not empty
                                for (String field : value) {
                                    if (field == null || field.trim().isEmpty()) {
                                        throw new ConfigException("Field names cannot be empty.");
                                    }
                                }
                            },
                            () -> "list of field names, e.g. <code>field1,field2,field3</code>"),
                    ConfigDef.Importance.HIGH,
                    "List of fields to convert from bytes to hex strings with 0x prefix.")
            .define(REPLACE_NULL_WITH_DEFAULT_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    "Whether to replace fields that have a default value and that are null to the default value. When set to true, the default value is used, otherwise null is used.")
            .define(CONVERSION_CACHE_SIZE_CONFIG,
                    ConfigDef.Type.INT,
                    1024,
                    ConfigDef.Range.atLeast(0),
                    ConfigDef.Importance.LOW,
                    "Maximum number of byte array to hex string conversions to cache. Set to 0 to disable caching.");

    private static final String PURPOSE = "converting bytes to hex strings";

    // Hex digits lookup table for faster conversion
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private List<String> fields;
    private Cache<Schema, Schema> schemaUpdateCache;
    private Cache<ByteArrayWrapper, String> conversionCache;
    private boolean replaceNullWithDefault;
    private int conversionCacheSize;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
        replaceNullWithDefault = config.getBoolean(REPLACE_NULL_WITH_DEFAULT_CONFIG);
        conversionCacheSize = config.getInt(CONVERSION_CACHE_SIZE_CONFIG);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));

        // Initialize conversion cache if enabled
        if (conversionCacheSize > 0) {
            conversionCache = new SynchronizedCache<>(new LRUCache<>(conversionCacheSize));
        }
    }

    @Override
    public R apply(R record) {
        if (operatingValue(record) == null) {
            return record;
        }

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        // Only create a new map if we actually need to transform something
        boolean needsTransformation = false;
        for (String field : fields) {
            if (value.containsKey(field) && value.get(field) != null) {
                needsTransformation = true;
                break;
            }
        }

        if (!needsTransformation) {
            return record;
        }

        // Create a new map with the transformed values
        final HashMap<String, Object> updatedValue = new HashMap<>(value);
        for (String field : fields) {
            if (value.containsKey(field)) {
                Object fieldValue = value.get(field);
                if (fieldValue != null) {
                    updatedValue.put(field, convertToHexString(fieldValue));
                }
            }
        }

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Schema schema = operatingSchema(record);

        // Get the updated schema (cached if we've seen this schema before)
        Schema updatedSchema = getOrBuildSchema(schema);

        // Create a new Struct with the updated schema
        final Struct updatedValue = new Struct(updatedSchema);

        // Copy all fields, transforming the ones that need it
        for (Field field : schema.fields()) {
            final String fieldName = field.name();
            final Object origFieldValue = getFieldValue(value, field);

            if (fields.contains(fieldName) && origFieldValue != null) {
                updatedValue.put(fieldName, convertToHexString(origFieldValue));
            } else {
                updatedValue.put(fieldName, origFieldValue);
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Object getFieldValue(Struct value, Field field) {
        if (replaceNullWithDefault) {
            return value.get(field);
        }
        return value.getWithoutDefault(field.name());
    }

    /**
     * Gets a cached schema or builds a new one where byte fields are converted to string fields
     */
    private Schema getOrBuildSchema(Schema schema) {
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema != null) {
            return updatedSchema;
        }

        // Create a new schema builder
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        // Add all fields, changing byte fields to string fields
        for (Field field : schema.fields()) {
            if (fields.contains(field.name())) {
                // Create a string schema for this field
                SchemaBuilder fieldBuilder = SchemaBuilder.string();

                // Copy optional flag
                if (field.schema().isOptional()) {
                    fieldBuilder.optional();
                }

                // Copy and convert default value if present
                if (field.schema().defaultValue() != null) {
                    Schema fieldSchema = field.schema();
                    fieldBuilder.defaultValue(convertToHexString(fieldSchema.defaultValue()));
                }

                builder.field(field.name(), fieldBuilder.build());
            } else {
                // Keep original schema for other fields
                builder.field(field.name(), field.schema());
            }
        }

        updatedSchema = builder.build();
        schemaUpdateCache.put(schema, updatedSchema);
        return updatedSchema;
    }

    /**
     * Converts various types of binary data to a hex string with 0x prefix
     */
    private String convertToHexString(Object value) {
        if (value == null) {
            return null;
        }

        // Handle byte arrays
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return getCachedHexString(bytes);
        }

        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) value;
            byte[] bytes = Utils.readBytes(buffer);
            return getCachedHexString(bytes);
        }

        // Handle Base64 encoded strings (common in Debezium)
        if (value instanceof String) {
            try {
                byte[] decoded = Base64.getDecoder().decode((String) value);
                return getCachedHexString(decoded);
            } catch (IllegalArgumentException e) {
                // Not Base64, return as is
                return (String) value;
            }
        }

        // For any other type, return string representation
        return value.toString();
    }

    /**
     * Gets a cached hex string or creates and caches a new one
     */
    private String getCachedHexString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }

        // If caching is disabled, convert directly
        if (conversionCacheSize <= 0 || conversionCache == null) {
            return toHexString(bytes);
        }

        // Wrap the byte array for use as a cache key
        ByteArrayWrapper key = new ByteArrayWrapper(bytes);

        // Try to get from cache
        String hexString = conversionCache.get(key);
        if (hexString == null) {
            // Not in cache, convert and cache the result
            hexString = toHexString(bytes);
            conversionCache.put(key, hexString);
        }

        return hexString;
    }

    /**
     * Converts a byte array to a hex string with 0x prefix
     * Using a highly optimized approach with a lookup table
     */
    private String toHexString(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }

        // Create result array with exact size needed (2 chars per byte + 2 for "0x")
        char[] hexChars = new char[bytes.length * 2 + 2];

        // Add "0x" prefix
        hexChars[0] = '0';
        hexChars[1] = 'x';

        // Convert bytes to hex characters
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            hexChars[i * 2 + 2] = HEX_ARRAY[v >>> 4];
            hexChars[i * 2 + 3] = HEX_ARRAY[v & 0x0F];
        }

        return new String(hexChars);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    /**
     * Gets the schema for the record we're operating on
     */
    protected abstract Schema operatingSchema(R record);

    /**
     * Gets the value for the record we're operating on
     */
    protected abstract Object operatingValue(R record);

    /**
     * Creates a new record based on the input record with the transformed schema and value
     */
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    /**
     * Implementation for transforming a record key
     */
    public static class Key<R extends ConnectRecord<R>> extends BytesToHexString<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue,
                    record.valueSchema(), record.value(), record.timestamp());
        }
    }

    /**
     * Implementation for transforming a record value
     */
    public static class Value<R extends ConnectRecord<R>> extends BytesToHexString<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                    updatedSchema, updatedValue, record.timestamp());
        }
    }

    /**
     * Wrapper for byte arrays to use as keys in the cache.
     * This handles the equals() and hashCode() properly for byte arrays.
     */
    private static class ByteArrayWrapper {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            this.data = data;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ByteArrayWrapper)) {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}