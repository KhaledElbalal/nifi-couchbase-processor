package khaled.processors.couchbaseProcessor;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonFactory;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonParser;
import com.couchbase.client.core.deps.com.fasterxml.jackson.core.JsonToken;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessorInitializationContext;

import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@Tags({"couchbase", "insert", "document"})
@CapabilityDescription("Inserts documents into a Couchbase collection from a FlowFile")
public class PutCouchbase extends AbstractProcessor {
    public static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Success relationship")
        .build();
    public static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Failure relationship")
        .build();
    
    public static final PropertyDescriptor CONNECTION_NAME = new PropertyDescriptor.Builder()
        .name("Couchbase Connection Name")
        .description("The name of the Couchbase connection")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
        .name("Couchbase Username")
        .description("The username for Couchbase connection")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("Couchbase Password")
        .description("The password for Couchbase connection")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor BUCKET = new PropertyDescriptor.Builder()
        .name("Couchbase Bucket")
        .description("The Couchbase bucket to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor SCOPE = new PropertyDescriptor.Builder()
        .name("Couchbase Scope")
        .description("The Couchbase scope to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor COLLECTION = new PropertyDescriptor.Builder()
        .name("Couchbase Collection")
        .description("The Couchbase collection to use")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private Cluster cluster;
    private Bucket bucket;
    private Collection collection;

    private Set<Relationship> relationships = new HashSet<>();
    private List<PropertyDescriptor> descriptors;

    @Override
    protected void init(ProcessorInitializationContext context) {
        super.init(context);
        this.descriptors = Arrays.asList(
            CONNECTION_NAME, USERNAME, PASSWORD, BUCKET, SCOPE, COLLECTION
        );

        this.descriptors = Collections.unmodifiableList(this.descriptors);
        final Set<Relationship> rels = new HashSet<>();
        rels.add(SUCCESS);
        rels.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        String connectionName = context.getProperty(CONNECTION_NAME).getValue();
        String username = context.getProperty(USERNAME).getValue();
        String password = context.getProperty(PASSWORD).getValue();
        String bucketName = context.getProperty(BUCKET).getValue();
        String scopeName = context.getProperty(SCOPE).getValue();
        String collectionName = context.getProperty(COLLECTION).getValue();

        cluster = Cluster.connect(connectionName, username, password);
        bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(10));
        collection = bucket.scope(scopeName).collection(collectionName);
    }

    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }
    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            long start = System.currentTimeMillis();            
            Runtime runtime = Runtime.getRuntime();

            long totalMemory = runtime.totalMemory();
            long maxMemory = runtime.maxMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;

            getLogger().info("Total memory: " + totalMemory);
            getLogger().info("Max memory: " + maxMemory);
            getLogger().info("Free memory: " + freeMemory);
            getLogger().info("Used memory: " + usedMemory);

            long startInsert = System.currentTimeMillis();


            try (InputStream inputStream = session.read(flowFile)) {
                JsonFactory jsonFactory = new JsonFactory();
                ObjectMapper objectMapper = new ObjectMapper();
            
                try (JsonParser jsonParser = jsonFactory.createParser(inputStream)) {
                    StringBuilder stringBuilder = new StringBuilder();
                    
                    String query = String.format("INSERT INTO `%s`.`%s`.`%s` (KEY, VALUE)", bucket.name(), collection.scopeName(), collection.name());
            
                    stringBuilder.append(query);

                    Boolean inserted = false;
            
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        if (jsonParser.currentToken() == JsonToken.START_OBJECT) {  
                            ObjectNode objectNode = objectMapper.readTree(jsonParser);
                            
                            String key = objectNode.get("COUCHBASE_META_DOCUMENT_ID").asText();
                            objectNode.remove("COUCHBASE_META_DOCUMENT_ID");
                            String value = " VALUES (\"" + key  + "\", " + objectNode.toString() + ")";

                            if(inserted) {
                                stringBuilder.append(",");
                            }
                            inserted = true;
                            stringBuilder.append(value);
                            if(stringBuilder.length() > 10000000) {
                                cluster.query(stringBuilder.toString());
                                stringBuilder = new StringBuilder();
                                stringBuilder.append(query);
                                inserted = false;
                            }
                        }
                    }


                    cluster.query(stringBuilder.toString());
                }
            }            

            session.remove(flowFile);
            flowFile = session.create(flowFile);

            getLogger().info("Query executed in: " + (System.currentTimeMillis() - startInsert) + "ms");
            long end = System.currentTimeMillis();

            getLogger().info("Time taken to insert documents: " + (end - start) + "ms");

            session.transfer(flowFile, SUCCESS);

        } catch (Exception e) {
            getLogger().error("Error inserting documents into Couchbase", e.getMessage());
            session.transfer(flowFile, FAILURE);
        }
    }
    @OnStopped
    public void OnStopped(ProcessContext context) {
        cluster.disconnect();
    }
}
