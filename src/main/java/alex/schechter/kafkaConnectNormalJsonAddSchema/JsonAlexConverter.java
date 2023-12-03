package alex.schechter.kafkaConnectNormalJsonAddSchema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.cache.Cache;
import io.confluent.connect.json.JsonSchemaData;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.confluent.connect.avro.AvroData.AVRO_TYPE_UNION;
import static io.confluent.connect.avro.AvroData.CONNECT_TYPE_PROP;
import static io.confluent.connect.avro.AvroData.AVRO_TYPE_ENUM;


public class JsonAlexConverter implements Converter{

    private SchemaRegistryClient schemaRegistry;
    static final String AVRO_LOGICAL_DECIMAL = "decimal";
    static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";

    static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
    static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    static final String AVRO_LOGICAL_DATE = "date";

    static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
    static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    private static final Map<String, Schema.Type> NON_AVRO_TYPES_BY_TYPE_CODE = new HashMap<>();

    static final Integer CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 64;
//    private Serializer serializer;
    private Deserializer deserializer;
    private static final Logger log = LoggerFactory.getLogger(JsonAlexConverter.class);
    private boolean isKey;

    // Public only for testing
    public JsonAlexConverter(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    public JsonAlexConverter(){}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

        log.info(configs.toString());

        JsonAlexConverterConfig jsonAlexConverterConfig = new JsonAlexConverterConfig(configs);

        this.isKey = isKey;
        if (schemaRegistry == null) {
            schemaRegistry = new CachedSchemaRegistryClient(
                    jsonAlexConverterConfig.getSchemaRegistryUrls(),
                    100
            );
        }

//        serializer = new Serializer(configs, schemaRegistry);
        deserializer = new Deserializer(configs, schemaRegistry);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return null;
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        return null;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            SchemaAndValue deserialized = deserializer.deserialize(topic, isKey, value);

            System.out.println("*******1111****************");
            System.out.println(deserialized.schema().toString());
            System.out.println("*******1111****************");
            System.out.println("*******1111****************");
            System.out.println("*******1111****************");
            System.out.println(deserialized.value().toString());
            if (deserialized.value() == null) {
                return SchemaAndValue.NULL;
            }

            Schema schema = deserialized.schema();

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode a = objectMapper.readTree((byte[]) deserialized.value());


            return new SchemaAndValue(schema, JsonSchemaData.toConnectData(schema, a));
        } catch (SerializationException e) {
            throw new DataException(String.format("Converting byte[] to Kafka Connect data failed due to "
                            + "serialization error of topic %s: ",
                    topic),
                    e
            );
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(
                    String.format("Failed to access JSON Schema data from "
                            + "topic %s : %s", topic, e.getMessage())
            );
        } catch (RestClientException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        SchemaAndValue deserialized = null;
        try {
            deserialized = deserializer.deserialize(topic, isKey, value);

            System.out.println("***********************");
            System.out.println(deserialized.schema().toString());
            System.out.println("***********************");
            System.out.println("***********************");
            System.out.println("***********************");
//            System.out.println(new String(deserialized.value()));

        } catch (RestClientException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (deserialized.value() == null) {
            return SchemaAndValue.NULL;
        }

        Schema schema = deserialized.schema();

        Object b = JsonSchemaData.toConnectData(schema, (JsonNode) deserialized.value());

        return new SchemaAndValue(schema, b);

    }


    private static class Deserializer extends AbstractKafkaJsonSchemaDeserializer {

        private static final Map<Schema.Type, org.apache.avro.Schema.Type> CONNECT_TYPES_TO_AVRO_TYPES
                = new HashMap<>();

        static {
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT32, org.apache.avro.Schema.Type.INT);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT64, org.apache.avro.Schema.Type.LONG);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT32, org.apache.avro.Schema.Type.FLOAT);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT64, org.apache.avro.Schema.Type.DOUBLE);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BOOLEAN, org.apache.avro.Schema.Type.BOOLEAN);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.STRING, org.apache.avro.Schema.Type.STRING);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BYTES, org.apache.avro.Schema.Type.BYTES);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.ARRAY, org.apache.avro.Schema.Type.ARRAY);
            CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.MAP, org.apache.avro.Schema.Type.MAP);
        }

        private static final Map<JsonNodeType, org.apache.avro.Schema.Type> TYPE_CONVERTERS = new HashMap<>();
        static {
            TYPE_CONVERTERS.put(JsonNodeType.STRING, org.apache.avro.Schema.Type.STRING);
            TYPE_CONVERTERS.put(JsonNodeType.ARRAY, org.apache.avro.Schema.Type.ARRAY);
            TYPE_CONVERTERS.put(JsonNodeType.BOOLEAN, org.apache.avro.Schema.Type.BOOLEAN);
            TYPE_CONVERTERS.put(JsonNodeType.NUMBER, org.apache.avro.Schema.Type.INT);
            TYPE_CONVERTERS.put(JsonNodeType.OBJECT, org.apache.avro.Schema.Type.RECORD);

        }
        private Cache<AvroSchema, Schema> toConnectSchemaCache;

        public Deserializer(SchemaRegistryClient client) {
            schemaRegistry = client;
            toConnectSchemaCache =
                    new SynchronizedCache<>(new LRUCache<>(1000));
        }

        public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client);
            configure(new KafkaJsonSchemaDeserializerConfig(configs), null);
        }

        public SchemaAndValue deserialize(String topic, boolean isKey, byte[] payload) throws RestClientException, IOException {

            SchemaMetadata schemaMetadata = schemaRegistry.getLatestSchemaMetadata(topic + "-value");
            AvroSchema schemaAndVersion = (AvroSchema) schemaRegistry.getSchemaBySubjectAndId(topic + "-value", schemaMetadata.getId());
            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaAndVersion.toString());
            Schema cachedSchema = toConnectSchemaCache.get(schemaAndVersion);
            JsonNode jsonData = objectMapper.readTree(payload);
            if (cachedSchema == null) {
                Triple<Schema, JsonNode, Boolean> schemaJsonNodePair = toConnectSchema(schema, (JsonNode) jsonData);
                cachedSchema = schemaJsonNodePair.getLeft();
                jsonData = schemaJsonNodePair.getMiddle();
                toConnectSchemaCache.put(schemaAndVersion, cachedSchema);
            }

            return new SchemaAndValue(
                    cachedSchema,
                    jsonData
            );
        }

        private Triple<Schema, JsonNode, Boolean> toConnectSchema(org.apache.avro.Schema schema, JsonNode jsonData) throws JsonProcessingException {

            String type = schema.getProp(CONNECT_TYPE_PROP);
            String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);

            SchemaBuilder builder = null;
            Boolean shouldCreateNewNode = false;

            Triple<Schema, JsonNode, Boolean> res = null;

            switch (schema.getType()) {
                case BOOLEAN:
                    builder = SchemaBuilder.bool().optional();
                    break;
                case BYTES:
                case FIXED:
                    if (AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
                        Object scaleNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP);
                        // In Avro the scale is optional and should default to 0
                        int scale = scaleNode instanceof Number ? ((Number) scaleNode).intValue() : 0;
                        builder = Decimal.builder(scale).optional();

                        Object precisionNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
                        if (null != precisionNode) {
                            if (!(precisionNode instanceof Number)) {
                                throw new DataException(AVRO_LOGICAL_DECIMAL_PRECISION_PROP
                                        + " property must be a JSON Integer."
                                        + " https://avro.apache.org/docs/1.9.1/spec.html#Decimal");
                            }
                            // Capture the precision as a parameter only if it is not the default
                            int precision = ((Number) precisionNode).intValue();
                            if (precision != CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT) {
                                builder.parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, String.valueOf(precision)).optional();
                            }
                        }
                    } else {
                        builder = SchemaBuilder.bytes().optional();
                    }
                    break;
                case DOUBLE:
                    builder = SchemaBuilder.float64().optional();
                    break;
                case FLOAT:
                    builder = SchemaBuilder.float32().optional();
                    break;
                case INT:
                    // INT is used for Connect's INT8, INT16, and INT32
                    if (type == null && logicalType == null) {
                        builder = SchemaBuilder.int32().optional();
                    } else if (logicalType != null) {
                        if (AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
                            builder = Date.builder().optional();
                        } else if (AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
                            builder = Time.builder().optional();
                        } else {
                            builder = SchemaBuilder.int32().optional();
                        }
                    } else {
                        Schema.Type connectType = NON_AVRO_TYPES_BY_TYPE_CODE.get(type);
                        if (connectType == null) {
                            throw new DataException("Connect type annotation for Avro int field is null");
                        }
                        builder = SchemaBuilder.type(connectType).optional();
                    }
                    break;
                case LONG:
                    if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
                        builder = Timestamp.builder().optional();
                    } else {
                        builder = SchemaBuilder.int64().optional();
                    }
                    break;
                case STRING:
                    builder = SchemaBuilder.string().optional();
                    break;

                case ARRAY:
                    builder = SchemaBuilder.struct().name(AVRO_TYPE_UNION);
                    org.apache.avro.Schema elemSchema = schema.getElementType();
                    Set<String> fieldNames = new HashSet<>();
                    // Special case for custom encoding of non-string maps as list of key-value records
                    for (int i = 0; i<jsonData.size(); i++){
                        JsonNode elem = jsonData.get(0);
                        res = toConnectSchema(elemSchema, elem);
                        if (res.getMiddle() != null && res.getRight()) {
                            ((ArrayNode) jsonData).remove(0);
                            ((ArrayNode) jsonData).add(res.getMiddle());
                            String fieldName = res.getMiddle().fieldNames().next();
                            if (!fieldNames.contains(res.getMiddle().fieldNames().next())) {
                                fieldNames.add(fieldName);
                                builder = builder.field(fieldName, res.getLeft().field(fieldName).schema());
                                builder.optional();
                            }
                        }

                    }
                    if (!fieldNames.isEmpty()){
                        Schema temp1 = builder.build();
                        builder.optional();
                        builder = SchemaBuilder.array(temp1);
                    }
                    else{
                        builder = SchemaBuilder.array(res.getLeft()).optional();
                    }

                    break;

                case MAP:
                    builder = SchemaBuilder.map(
                            Schema.STRING_SCHEMA,
                            toConnectSchema(schema.getValueType(), jsonData).getLeft()
                    ).optional();
                    break;
                case RECORD:
                    builder = SchemaBuilder.struct();
                    for (org.apache.avro.Schema.Field field : schema.getFields()) {
                        res = toConnectSchema(field.schema(), jsonData.get(field.name()));
                        Schema fieldSchema = res.getLeft();
                        if (res.getMiddle() != null) {
                            ((ObjectNode)jsonData).put(field.name(), res.getMiddle());
                        }
                        builder.field(field.name(), fieldSchema);
                    }
                    break;
                case ENUM:
                    // enums are unwrapped to strings and the original enum is not preserved
                    builder = SchemaBuilder.string();
                    builder.parameter(AVRO_TYPE_ENUM, schema.getFullName());
                    for (String enumSymbol : schema.getEnumSymbols()) {
                        builder.parameter(AVRO_TYPE_ENUM + "." + enumSymbol, enumSymbol).optional();
                    }
                    break;

                case UNION:
                    builder = SchemaBuilder.struct().name(AVRO_TYPE_UNION).optional();

                    int index = schema.getTypes().stream().map(field -> field.getType()).collect(Collectors.toList()).indexOf(TYPE_CONVERTERS.get(jsonData.getNodeType()));
                    assert index != -1;

                    builder.optional();

                    String temp = "{\"member" + index + "\":" + jsonData + "}";
                    jsonData = objectMapper.readTree(temp);

                    Triple<Schema, JsonNode, Boolean> schemaJson = toConnectSchema(schema.getTypes().get(index), jsonData);

                    jsonData = schemaJson.getMiddle();
                    builder.field(
                            jsonData.fieldNames().next(),
                            schemaJson.getLeft()
                    ).optional();

                    shouldCreateNewNode = true;

                    break;

            }


            assert builder != null;
            return Triple.of( builder.build(), jsonData, shouldCreateNewNode);
        }

    }
}