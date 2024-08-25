package khaled.processors.couchbaseProcessor;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.query.QueryResult;

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
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
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

    public static final PropertyDescriptor DOCUMENT_ID = new PropertyDescriptor.Builder()
        .name("Document ID")
        .description("The ID of the document to insert")
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
            CONNECTION_NAME, USERNAME, PASSWORD, BUCKET, SCOPE, COLLECTION, DOCUMENT_ID
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
            String documentIdPrefix = context.getProperty(DOCUMENT_ID).getValue();

            // Properly close the InputStream using try-with-resources
            final String flowFileContent;
            try (InputStream inputStream = session.read(flowFile)) {
                flowFileContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }

            // Parse the content as a JSON array
            JsonArray jsonArray;
            try {
                jsonArray = JsonArray.fromJson(flowFileContent);
            } catch (Exception e) {
                getLogger().error("Invalid JSON array structure in FlowFile content", e);
                session.transfer(flowFile, FAILURE);
                return;
            }

            // Insert the JSON array into the Couchbase collection

            String query = String.format("INSERT INTO `%s`.`%s`.`%s` (KEY, VALUE)", bucket.name(), collection.scopeName(), collection.name());

            StringWriter arrayContent = new StringWriter();
            arrayContent.write(query);
            for (int i = 0; i < jsonArray.size(); i++) {
                if (i > 0) {
                    arrayContent.write(", ");
                }
                arrayContent.write(" VALUES ");
                arrayContent.write("('" + documentIdPrefix + i + "', " + jsonArray.get(i).toString() + ")");
            }
            
            QueryResult queryContent = cluster.query(arrayContent.toString());
            getLogger().info("Inserted documents: " + queryContent.toString());
            long end = System.currentTimeMillis();
            getLogger().info("Time taken to insert documents: " + (end - start) + "ms");

            
            session.transfer(flowFile, SUCCESS);

        } catch (Exception e) {
            getLogger().error("Error inserting documents into Couchbase", e);
            session.transfer(flowFile, FAILURE);
        }
    }
    @OnStopped
    public void OnStopped(ProcessContext context) {
        cluster.disconnect();
    }
}
