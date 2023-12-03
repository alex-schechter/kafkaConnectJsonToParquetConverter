package alex.schechter.kafkaConnectNormalJsonAddSchema;

import com.fasterxml.jackson.databind.JsonNode;
import io.confluent.common.config.ConfigDef;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JsonAlexConverterConfig extends AbstractKafkaSchemaSerDeConfig {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    public static final String
            SCHEMA_REGISTRY_URL_DOC =
            "URL for schema registry instances that can be used to register "
                    + "or look up schemas. ";

    public JsonAlexConverterConfig(Map<?, ?> props) {

        super(baseConfigDef(), props);
    }

    public static class Builder {

        private Map<String, Object> props = new HashMap<>();

        public Builder with(String key, Object value) {
            props.put(key, value);
            return this;
        }

        public JsonAlexConverterConfig build() {
            return new JsonAlexConverterConfig(props);
        }
    }
}