package alex.schechter.kafkaConnectNormalJsonAddSchema;

import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.json.JsonSchemaAndValue;
import org.apache.kafka.connect.data.Schema;

import java.util.Objects;

public class AvroSchemaAndValue {
    private final Schema schema;
    private final Object value;

    public AvroSchemaAndValue(Schema schema, Object value) {
        this.schema = schema;
        this.value = value;
    }

    public Schema getSchema() {
        return this.schema;
    }

    public Object getValue() {
        return this.value;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            AvroSchemaAndValue that = (AvroSchemaAndValue)o;
            return Objects.equals(this.schema, that.schema) && Objects.equals(this.value, that.value);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.schema, this.value});
    }
}
