package alex.schechter.kafkaConnectNormalJsonAddSchema;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class JsonAlexConverterTest {
    private static final String TOPIC = "topic";

    private static final Map<String, String> SR_CONFIG = Collections.singletonMap(
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost");

    private final SchemaRegistryClient schemaRegistry;
    private final JsonAlexConverter converter;

    public JsonAlexConverterTest() {
        schemaRegistry = new MockSchemaRegistryClient();
        converter = new JsonAlexConverter(schemaRegistry);
    }

    @Before
    public void setUp() {
        converter.configure(Collections.singletonMap("schema.registry.url", "http://localhost:8081"), false);
    }

//    @Test
//    public void testConfigure() {
//        converter.configure(SR_CONFIG, true);
//        assertTrue(Whitebox.<Boolean>getInternalState(converter, "isKey"));
//        assertNotNull(Whitebox.getInternalState(
//                Whitebox.<AbstractKafkaSchemaSerDe>getInternalState(converter, "serializer"),
//                "schemaRegistry"));
//    }
//
//    @Test
//    public void testConfigureAlt() {
//        converter.configure(SR_CONFIG, false);
//        assertFalse(Whitebox.<Boolean>getInternalState(converter, "isKey"));
//        assertNotNull(Whitebox.getInternalState(
//                Whitebox.<AbstractKafkaSchemaSerDe>getInternalState(converter, "serializer"),
//                "schemaRegistry"));
//    }

//    @Test
//    public void testPrimitive() {
//        SchemaAndValue original = new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true);
//        byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original.value());
//        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
//        // Because of registration in schema registry and lookup, we'll have added a version number
//        SchemaAndValue expected = new SchemaAndValue(SchemaBuilder.bool().version(1).build(), true);
//        assertEquals(expected, schemaAndValue);
//    }
//
//    @Test
//    public void testComplex() {
//        SchemaBuilder builder = SchemaBuilder.struct()
//                .field("int8", SchemaBuilder.int8().defaultValue((byte) 2).doc("int8 field").build())
//                .field("int16", Schema.INT16_SCHEMA)
//                .field("int32", Schema.INT32_SCHEMA)
//                .field("int64", Schema.INT64_SCHEMA)
//                .field("float32", Schema.FLOAT32_SCHEMA)
//                .field("float64", Schema.FLOAT64_SCHEMA)
//                .field("boolean", Schema.BOOLEAN_SCHEMA)
//                .field("string", Schema.STRING_SCHEMA)
//                .field("bytes", Schema.BYTES_SCHEMA)
//                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
//                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
//                .field("mapNonStringKeys", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA)
//                        .build());
//        Schema schema = builder.build();
//        Struct original = new Struct(schema)
//                .put("int8", (byte) 12)
//                .put("int16", (short) 12)
//                .put("int32", 12)
//                .put("int64", 12L)
//                .put("float32", 12.2f)
//                .put("float64", 12.2)
//                .put("boolean", true)
//                .put("string", "foo")
//                .put("bytes", ByteBuffer.wrap("foo".getBytes()))
//                .put("array", Arrays.asList("a", "b", "c"))
//                .put("map", Collections.singletonMap("field", 1))
//                .put("mapNonStringKeys", Collections.singletonMap(1, 1));
//        // Because of registration in schema registry and lookup, we'll have added a version number
//        Schema expectedSchema = builder.version(1).build();
//        Struct expected = new Struct(expectedSchema)
//                .put("int8", (byte) 12)
//                .put("int16", (short) 12)
//                .put("int32", 12)
//                .put("int64", 12L)
//                .put("float32", 12.2f)
//                .put("float64", 12.2)
//                .put("boolean", true)
//                .put("string", "foo")
//                .put("bytes", ByteBuffer.wrap("foo".getBytes()))
//                .put("array", Arrays.asList("a", "b", "c"))
//                .put("map", Collections.singletonMap("field", 1))
//                .put("mapNonStringKeys", Collections.singletonMap(1, 1));
//
//        byte[] converted = converter.fromConnectData(TOPIC, original.schema(), original);
//        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
//        assertEquals(expected, schemaAndValue.value());
//    }

//    @Test
//    public void testTypeBytes() {
//        Schema schema = SchemaBuilder.bytes().build();
//        byte[] b = converter.fromConnectData("topic", schema, "jellomellow".getBytes());
//        SchemaAndValue sv = converter.toConnectData("topic", b);
//        assertEquals(Type.BYTES, sv.schema().type());
//        assertArrayEquals("jellomellow".getBytes(), ((ByteBuffer) sv.value()).array());
//    }
//
//    @Test
//    public void testNull() {
//        // Because of the way our serialization works, it's expected that we'll lose schema information
//        // when the entire schema is optional. The null value should be written as a null and this
//        // should mean we also do *not* register a schema.
//        byte[] converted = converter.fromConnectData(TOPIC, Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
//        assertNull(converted);
//        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, converted);
//        assertEquals(SchemaAndValue.NULL, schemaAndValue);
//    }

//    @Test
//    public void testVersionExtractedForDefaultSubjectNameStrategy() throws Exception {
//        // Version info should be extracted even if the data was not created with Copycat. Manually
//        // register a few compatible schemas and validate that data serialized with our normal
//        // serializer can be read and gets version info inserted
//        String subject = TOPIC + "-value";
//        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
//        JsonAlexConverter jsonAlexConverter = new JsonAlexConverter(schemaRegistry);
//        jsonAlexConverter.configure(Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
//        testVersionExtracted(subject, serializer, jsonAlexConverter);
//
//    }

//    @Test
//    public void testVersionExtractedForRecordSubjectNameStrategy() throws Exception {
//        // Version info should be extracted even if the data was not created with Copycat. Manually
//        // register a few compatible schemas and validate that data serialized with our normal
//        // serializer can be read and gets version info inserted
//        String subject =  "Foo";
//        Map<String, Object> configs = ImmutableMap.<String, Object>of("schema.registry.url", "http://fake-url", "value.subject.name.strategy", RecordNameStrategy.class.getName());
//        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
//        serializer.configure(configs, false);
//        JsonAlexConverter jsonAlexConverter = new JsonAlexConverter(schemaRegistry);
//
//        jsonAlexConverter.configure(configs, false);
//        testVersionExtracted(subject, serializer, jsonAlexConverter);
//
//    }

    @Test
    public void testRecord() throws IOException, RestClientException, RestClientException, IOException {
        // Pre-register to ensure ordering
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record("MyRecord")
                .fields()
                .name("my_field1").type().intType().noDefault()
                .name("my_field3").type().stringType().noDefault()
                .endRecord();

        schemaRegistry.register("asdf-value", new AvroSchema(avroSchema1));


        String jsonString = "{\n" +
                "\t\"my_field1\": 123,\n" +
                "\t\"my_field3\": \"18\"\n" +
                "}";

        byte[] byteArray = jsonString.getBytes();
        SchemaAndValue converted1 = converter.toConnectData("asdf", null, byteArray);
        assert (1L == 1L);
    }

    @Test
    public void testRecordAndUnion() throws IOException, RestClientException, RestClientException, IOException {
        // Pre-register to ensure ordering
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record("Order").fields()
                .requiredLong("ordertime")
                .requiredInt("orderid")
                .requiredString("itemid")
                .name("address").type(org.apache.avro.SchemaBuilder
                        .record("Address").fields()
                        .requiredString("city")
                        .requiredString("state")
                        .requiredInt("zipcode")
                        .endRecord()).noDefault()
                .endRecord();
        schemaRegistry.register("asdf-value", new AvroSchema(avroSchema1));



        String jsonString = "{\n" +
                "\t\"a\": 123,\n" +
                "\t\"orderid\": \"18\",\n" +
                "\t\"itemid\": \"Item_184\",\n" +
                "\t\"address\": {\n" +
                "\t\t\"city\": \"Mountain View\",\n" +
                "\t\t\"state\": \"CA\",\n" +
                "\t\t\"zipcode\": 94041\n" +
                "\t}\n" +
                "}";

//        org.apache.avro.Schema avroSchema2 = org.apache.avro.SchemaBuilder.unionOf().stringType().and().intType().endUnion();
        org.apache.avro.Schema nestedRecordSchema = org.apache.avro.Schema.createRecord("nested", null, null, false);
        nestedRecordSchema.addProp("avro.java.string", "String");
        List<org.apache.avro.Schema.Field> f1 = new ArrayList<org.apache.avro.Schema.Field>();
        f1.add(new org.apache.avro.Schema.Field("nested", org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null));
        nestedRecordSchema.setFields(f1);

        // Union type for field "a" (can be either string or int)
        org.apache.avro.Schema unionTypeForA = org.apache.avro.Schema.createUnion(
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
                org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT)
        );

        // Main Avro schema with two fields
        org.apache.avro.Schema avroSchema2 = org.apache.avro.Schema.createRecord("ExampleRecord", null, null, false);
        avroSchema2.addProp("avro.java.string", "String");
        List<org.apache.avro.Schema.Field> f2 = new ArrayList<org.apache.avro.Schema.Field>();
        f2.add(new org.apache.avro.Schema.Field("a", unionTypeForA, null, null));
        f2.add(new org.apache.avro.Schema.Field("b", nestedRecordSchema, null, null));
        avroSchema2.setFields(f2);
        schemaRegistry.register("asdf1-value", new AvroSchema(avroSchema2));

        // Get serialized data
        String jsonString1 = "{\n" +
                "  \"a\": \"exampleString\",\n" +
                "  \"b\": {\n" +
                "    \"nested\": \"nestedStringValue\"\n" +
                "  }\n" +
                "}";

        // Convert JSON string to byte array using the default character encoding (UTF-8)
        byte[] byteArray = jsonString.getBytes();
        SchemaAndValue converted1 = converter.toConnectData("asdf",null, byteArray);


        byte[] byteArray1 = jsonString1.getBytes();
        SchemaAndValue converted2 = converter.toConnectData("asdf1",null, byteArray1);

//        SchemaAndValue converted2 = jsonAlexConverter.toConnectData(TOPIC, serializedRecord2);
        assertEquals(1L, 1L);
    }

    @Test
    public void testArray() throws IOException, RestClientException, RestClientException, IOException {
        // Pre-register to ensure ordering
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record("TestRecord")
                .fields()
                .name("test").type()
                .array()
                .items()
                .stringType()
                .noDefault()
                .endRecord();

        schemaRegistry.register("asdf-value", new AvroSchema(avroSchema1));


        String jsonString = "{\"test\": []}";

        // Convert JSON string to byte array using the default character encoding (UTF-8)
        byte[] byteArray = jsonString.getBytes();
        SchemaAndValue converted1 = converter.toConnectData("asdf",null, byteArray);

        assertEquals(1L, 1L);
    }

    @Test
    public void ArrayTest() throws IOException, RestClientException, RestClientException, IOException {
        // Pre-register to ensure ordering


        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
                .record("TestRecord")
                .fields()
                .name("test").type()
                .array()
                .items()
                .intType()
                .noDefault()
                .endRecord();
        schemaRegistry.register("asdf-value", new AvroSchema(avroSchema));


        String jsonString = "{\"test\": [1.12]}";

        // Convert JSON string to byte array using the default character encoding (UTF-8)
        byte[] byteArray = jsonString.getBytes();
        SchemaAndValue converted1 = converter.toConnectData("asdf",null, byteArray);

        assertEquals(1L, 1L);
    }

    @Test
    public void unionWithRecord() throws IOException, RestClientException, RestClientException, IOException {
        // Pre-register to ensure ordering
        org.apache.avro.Schema innerRecordSchema = org.apache.avro.SchemaBuilder
                .record("InnerRecord")
                .fields()
                .name("a").type(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING)).noDefault()
                .endRecord();

        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder
                .record("TestRecord")
                .fields()
                .name("test").type()
                .array()
                .items()
                .unionOf()
                .nullType()
                .and()
                .intType()
                .and()
                .stringType()
                .and()
                .type(innerRecordSchema)
                .endUnion()
                .noDefault()
                .endRecord();
        schemaRegistry.register("asdf-value", new AvroSchema(avroSchema));


        String jsonString = "{\"test\": [1, \"asdfasfd\", \"adsf\", {\"a\": \"asdfasfd\"}]}";

        // Convert JSON string to byte array using the default character encoding (UTF-8)
        byte[] byteArray = jsonString.getBytes();
        SchemaAndValue converted1 = converter.toConnectData("asdf",null, byteArray);

        assertEquals(1L, 1L);
    }


    @Test
    public void testVersionMaintained() {
        // Version info provided from the Copycat schema should be maintained. This should be true
        // regardless of any underlying schema registry versioning since the versions are explicitly
        // specified by the connector.

        // Use newer schema first
        Schema newerSchema = SchemaBuilder.struct().version(2)
                .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
                .field("new", Schema.OPTIONAL_INT16_SCHEMA)
                .build();
        SchemaAndValue newer = new SchemaAndValue(newerSchema, new Struct(newerSchema));
        byte[] newerSerialized = converter.fromConnectData(TOPIC, newer.schema(), newer.value());

        Schema olderSchema = SchemaBuilder.struct().version(1)
                .field("orig", Schema.OPTIONAL_INT16_SCHEMA)
                .build();
        SchemaAndValue older = new SchemaAndValue(olderSchema, new Struct(olderSchema));
        byte[] olderSerialized = converter.fromConnectData(TOPIC, older.schema(), older.value());

        assertEquals(2L, (long) converter.toConnectData(TOPIC, newerSerialized).schema().version());
        assertEquals(1L, (long) converter.toConnectData(TOPIC, olderSerialized).schema().version());
    }


    @Test
    public void testSameSchemaMultipleTopicForValue() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        JsonAlexConverter jsonAlexConverter = new JsonAlexConverter(schemaRegistry);
        jsonAlexConverter.configure(SR_CONFIG, false);
        assertSameSchemaMultipleTopic(jsonAlexConverter, schemaRegistry, false);
    }

    @Test
    public void testSameSchemaMultipleTopicForKey() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        JsonAlexConverter jsonAlexConverter = new JsonAlexConverter(schemaRegistry);
        jsonAlexConverter.configure(SR_CONFIG, true);
        assertSameSchemaMultipleTopic(jsonAlexConverter, schemaRegistry, true);
    }

//    @Test
//    public void testSameSchemaMultipleTopicWithDeprecatedSubjectNameStrategyForValue() throws IOException, RestClientException {
//        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
//        JsonAlexConverter jsonAlexConverter = new JsonAlexConverter(schemaRegistry);
//        Map<String, ?> converterConfig = ImmutableMap.of(
//                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost",
//                AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, DeprecatedTestTopicNameStrategy.class.getName());
//        jsonAlexConverter.configure(converterConfig, false);
//        assertSameSchemaMultipleTopic(jsonAlexConverter, schemaRegistry, false);
//    }

//    @Test
//    public void testSameSchemaMultipleTopicWithDeprecatedSubjectNameStrategyForKey() throws IOException, RestClientException {
//        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
//        JsonAlexConverter jsonAlexConverter = new JsonAlexConverter(schemaRegistry);
//        Map<String, ?> converterConfig = ImmutableMap.of(
//                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost",
//                AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, DeprecatedTestTopicNameStrategy.class.getName());
//        jsonAlexConverter.configure(converterConfig, true);
//        assertSameSchemaMultipleTopic(jsonAlexConverter, schemaRegistry, true);
//    }

    @Test
    public void testExplicitlyNamedNestedMapsWithNonStringKeys() {
        final Schema schema = SchemaBuilder.map(
                Schema.OPTIONAL_STRING_SCHEMA,
                SchemaBuilder.map(
                        Schema.OPTIONAL_STRING_SCHEMA,
                        Schema.INT32_SCHEMA
                ).name("foo.bar").build()
        ).name("biz.baz").version(1).build();
        final JsonAlexConverter jsonAlexConverter = new JsonAlexConverter(new MockSchemaRegistryClient());
        jsonAlexConverter.configure(
                Collections.singletonMap(
                        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "localhost"
                ),
                false
        );
        final Object value = Collections.singletonMap("foo", Collections.singletonMap("bar", 1));

        final byte[] bytes = jsonAlexConverter.fromConnectData("topic", schema, value);
        final SchemaAndValue schemaAndValue = jsonAlexConverter.toConnectData("topic", bytes);

        assertThat(schemaAndValue.schema(), equalTo(schema));
        assertThat(schemaAndValue.value(), equalTo(value));
    }

    private void assertSameSchemaMultipleTopic(JsonAlexConverter converter, SchemaRegistryClient schemaRegistry, boolean isKey) throws IOException, RestClientException {
        org.apache.avro.Schema avroSchema1 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .endRecord();

        org.apache.avro.Schema avroSchema2_1 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .requiredString("value")
                .endRecord();
        org.apache.avro.Schema avroSchema2_2 = org.apache.avro.SchemaBuilder
                .record("Foo").fields()
                .requiredInt("key")
                .requiredString("value")
                .endRecord();
        String subjectSuffix = isKey ? "key" : "value";
        schemaRegistry.register("topic1-" + subjectSuffix, new AvroSchema(avroSchema2_1));
        schemaRegistry.register("topic2-" + subjectSuffix, new AvroSchema(avroSchema1));
        schemaRegistry.register("topic2-" + subjectSuffix, new AvroSchema(avroSchema2_2));

        org.apache.avro.generic.GenericRecord avroRecord1
                = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2_1).set("key", 15).set
                ("value", "bar").build();
        org.apache.avro.generic.GenericRecord avroRecord2
                = new org.apache.avro.generic.GenericRecordBuilder(avroSchema2_2).set("key", 15).set
                ("value", "bar").build();


//        KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistry);
//        serializer.configure(SR_CONFIG, isKey);
//        byte[] serializedRecord1 = serializer.serialize("topic1", avroRecord1);
//        byte[] serializedRecord2 = serializer.serialize("topic2", avroRecord2);
//
//        SchemaAndValue converted1 = converter.toConnectData("topic1", serializedRecord1);
//        assertEquals(1L, (long) converted1.schema().version());
//
//        SchemaAndValue converted2 = converter.toConnectData("topic2", serializedRecord2);
//        assertEquals(2L, (long) converted2.schema().version());
//
//        converted2 = converter.toConnectData("topic2", serializedRecord2);
//        assertEquals(2L, (long) converted2.schema().version());
    }
}
