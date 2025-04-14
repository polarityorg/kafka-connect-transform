package io.equaleyes.kafka.connect.transforms.questdb;

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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * A Kafka Connect SMT that converts bytes fields to QuestDB long256 type (String representation).
 * The transformation converts PostgreSQL bytea data to a hex string with 'i' suffix as required by QuestDB.
 *
 * @param <R> The record type
 */
public abstract class BytesToLong256<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    public static final String FIELDS_CONFIG = "fields";
    public static final String REPLACE_NULL_WITH_DEFAULT_CONFIG = "replace.null.with.default";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.LambdaValidator.with(
                            (name, valueObject) -> {
                                @SuppressWarnings("unchecked")
                                List<String> value = (List<String>) valueObject;
                                if (value == null || value.isEmpty()) {
                                    throw new ConfigException("Must specify at least one field to convert.");
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
                    "List of fields to convert from bytes to long256 type.")
            .define(REPLACE_NULL_WITH_DEFAULT_CONFIG,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.MEDIUM,
                    "Whether to replace fields that have a default value and that are null to the default value.");

    private static final String PURPOSE = "converting bytes to long256 type";

    private List<String> fields;
    private Cache<Schema, Schema> schemaUpdateCache;
    private boolean replaceNullWithDefault;

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fields = config.getList(FIELDS_CONFIG);
        replaceNullWithDefault = config.getBoolean(REPLACE_NULL_WITH_DEFAULT_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
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
                    updatedValue.put(field, convertToLong256(fieldValue));
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

            // Process fields to be transformed
            if (fields.contains(fieldName)) {
                if (origFieldValue != null) {
                    updatedValue.put(fieldName, convertToLong256(origFieldValue));
                } else if (updatedSchema.field(fieldName).schema().isOptional()) {
                    // Only put null if the field is optional
                    updatedValue.put(fieldName, null);
                }
                // Skip non-optional fields with null values to avoid exceptions
            } else {
                // For fields that shouldn't be transformed, just copy the value
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
     * for QuestDB long256 representation
     */
    private Schema getOrBuildSchema(Schema schema) {
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if (updatedSchema != null) {
            return updatedSchema;
        }

        // Create a new schema builder
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        // Add all fields, changing byte fields to string fields for long256
        for (Field field : schema.fields()) {
            if (fields.contains(field.name())) {
                // Create a schema for the string field (QuestDB long256 is represented as a string)
                SchemaBuilder fieldBuilder = SchemaBuilder.string();

                // Copy optional flag
                if (field.schema().isOptional()) {
                    fieldBuilder.optional();
                }

                // Copy and convert default value if present
                if (field.schema().defaultValue() != null) {
                    Schema fieldSchema = field.schema();
                    fieldBuilder.defaultValue(convertToLong256(fieldSchema.defaultValue()));
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
     * Converts various types of binary data to a QuestDB long256 representation
     * (hex string with 'i' suffix)
     */
    private String convertToLong256(Object value) {
        if (value == null) {
            return null;
        }

        // Handle byte arrays
        if (value instanceof byte[]) {
            return bytesToLong256((byte[]) value);
        }

        if (value instanceof ByteBuffer) {
            ByteBuffer buffer = (ByteBuffer) value;
            byte[] bytes = Utils.readBytes(buffer);
            return bytesToLong256(bytes);
        }

        // Handle strings - for existing hex values or Base64
        if (value instanceof String) {
            String strValue = (String) value;

            // If it's already a QuestDB long256 hex string (with 'i' suffix), normalize it
            if ((strValue.startsWith("0x") || strValue.startsWith("0X")) && strValue.endsWith("i")) {
                try {
                    // Extract hex part without prefix and suffix
                    String hexPart = strValue.substring(2, strValue.length() - 1);
                    // Convert to BigInteger to normalize (remove leading zeros)
                    BigInteger bigInt = new BigInteger(hexPart, 16);
                    // Return normalized format
                    return "0x" + bigInt.toString(16).toLowerCase() + "i";
                } catch (NumberFormatException e) {
                    // If there's an issue parsing, just return as is
                    return strValue;
                }
            }

            // If it's a regular hex string, normalize and add 'i' suffix
            if (strValue.startsWith("0x") || strValue.startsWith("0X")) {
                try {
                    // Extract hex part without prefix
                    String hexPart = strValue.substring(2);
                    // Convert to BigInteger to normalize (remove leading zeros)
                    BigInteger bigInt = new BigInteger(hexPart, 16);
                    // Return normalized format
                    return "0x" + bigInt.toString(16).toLowerCase() + "i";
                } catch (NumberFormatException e) {
                    // If there's an issue parsing, just add suffix
                    return strValue + "i";
                }
            }

            // Try to decode as Base64
            try {
                byte[] decoded = Base64.getDecoder().decode(strValue);
                return bytesToLong256(decoded);
            } catch (IllegalArgumentException e) {
                // Not Base64, just convert to long256 format
                return "0x" + strValue + "i";
            }
        }

        // For any other type, convert to string and add format
        return "0x" + value.toString() + "i";
    }

    /**
     * Converts a byte array to QuestDB long256 hex string with 'i' suffix
     */
    private String bytesToLong256(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "0x8000000000000000800000000000000080000000000000008000000000000000i";
        }

        // Convert byte array to BigInteger for proper hex representation
        BigInteger bigInt = new BigInteger(1, bytes);

        // Convert to hex string and add QuestDB long256 format
        // This will automatically remove leading zeros
        return "0x" + bigInt.toString(16).toLowerCase() + "i";
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
    public static class Key<R extends ConnectRecord<R>> extends BytesToLong256<R> {
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
    public static class Value<R extends ConnectRecord<R>> extends BytesToLong256<R> {
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
}