package khaled.processors.couchbaseProcessor;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonObject;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonToken;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;

@Tags({"couchbase", "insert", "document"})
@CapabilityDescription("Inserts documents into a Couchbase collection from a FlowFile using Reactor")
public class PutCouchbase extends AbstractCouchbaseProcessor {
    private List<PropertyDescriptor> descriptors = Collections.unmodifiableList(Arrays.asList(
        CONNECTION_NAME, USERNAME, PASSWORD, BUCKET, SCOPE, COLLECTION
    ));

    private ReactiveCollection reactiveCollection;


    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        Bucket bucket = cluster.bucket(context.getProperty(BUCKET).getValue());
        Collection collection = bucket.scope(context.getProperty(SCOPE).getValue()).collection(context.getProperty(COLLECTION).getValue());
        reactiveCollection = collection.reactive();
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try (InputStream inputStream = session.read(flowFile)) {
            JsonFactory jsonFactory = new JsonFactory();
            ObjectMapper objectMapper = new ObjectMapper();
            List<JsonDocument> documents = new ArrayList<>();
            
            try (JsonParser jsonParser = jsonFactory.createParser(inputStream)) {
                while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                    if (jsonParser.currentToken() == JsonToken.START_OBJECT) {
                        ObjectNode objectNode = objectMapper.readTree(jsonParser);
                        String key = objectNode.get("COUCHBASE_META_DOCUMENT_ID").asText();
                        objectNode.remove("COUCHBASE_META_DOCUMENT_ID");
                        documents.add(new JsonDocument(key, JsonObject.fromJson(objectNode.toString())));
                    }
                }
            }

            Flux.fromIterable(documents)
                .flatMap(doc -> reactiveCollection.insert(doc.id(), doc.content())
                    .onErrorResume(e -> {
                        getLogger().error("Error inserting document {}", doc.id(), e);
                        return Mono.empty();
                    })
                )
                .blockLast();

            session.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            getLogger().error("Error inserting documents into Couchbase", e);
            session.transfer(flowFile, FAILURE);
        }
    }

    @OnStopped
    public void onStopped() {
        cluster.disconnect();
    }
}

record JsonDocument(String id, JsonObject content) {
    @Override
    public String toString() {
        return "JsonDocument{id='" + id + "', content=" + content + "}";
    }
}
