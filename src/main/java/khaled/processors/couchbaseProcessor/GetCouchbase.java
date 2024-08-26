package khaled.processors.couchbaseProcessor;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Collection;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@Tags({"couchbase", "query", "document"})
@CapabilityDescription("Queries all documents from a Couchbase collection and writes the results to a FlowFile")
public class GetCouchbase extends AbstractProcessor {
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

    public static final PropertyDescriptor BATCH_SIZE_MB = new PropertyDescriptor.Builder()
        .name("Batch Size (MB)")
        .description("The size of each batch in MB")
        .defaultValue("100")
        .addValidator(StandardValidators.LONG_VALIDATOR)
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
            CONNECTION_NAME, USERNAME, PASSWORD, BUCKET, SCOPE, COLLECTION, BATCH_SIZE_MB
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
        FlowFile flowFile = session.create();
        if (flowFile == null) {
            return;
        }

        try {
            flowFile = session.append(flowFile, out -> out.write("[".getBytes(StandardCharsets.UTF_8)));

            long start = System.currentTimeMillis();

            QueryResult countOfDocuments = cluster.query(String.format("SELECT COUNT(*) FROM `%s`.`%s`.`%s`", bucket.name(), collection.scopeName(), collection.name()));
            long count = countOfDocuments.rowsAsObject().get(0).getLong("$1");

            QueryResult documentSize = cluster.query(String.format("SELECT AVG(LENGTH(ENCODE_JSON(`%s`.`%s`.`%s`))) FROM `%s`.`%s`.`%s` LIMIT 100", bucket.name(), collection.scopeName(), collection.name(), bucket.name(), collection.scopeName(), collection.name()));
            long averageSize = 8 * documentSize.rowsAsObject().get(0).getLong("$1");

            long megabyteLimit = 1024 * 1024 * Long.parseLong(context.getProperty(BATCH_SIZE_MB).getValue());
            long batchSize = megabyteLimit / averageSize;
            long offset = 0;
            long limit = batchSize;
            for (long i = 0; i < count; i += batchSize) {
                QueryResult result = cluster.query(String.format("SELECT * FROM `%s`.`%s`.`%s` LIMIT %d OFFSET %d", bucket.name(), collection.scopeName(), collection.name(), limit, offset));
                offset += batchSize;
                limit += batchSize;
                String resultContentString = result.rowsAsObject().toString();
                resultContentString = resultContentString.substring(1, resultContentString.length() - 1);
                if(i + batchSize < count) {
                    resultContentString += ",";
                }
                final String processedResult = resultContentString;
                flowFile = session.append(flowFile, out -> out.write(processedResult.getBytes(StandardCharsets.UTF_8)));
            }

            flowFile = session.append(flowFile, out -> out.write("]".getBytes(StandardCharsets.UTF_8)));

            // Log end time
            long end = System.currentTimeMillis();
            getLogger().info("Time taken to query documents from Couchbase: " + (end - start) + "ms");

            session.transfer(flowFile, SUCCESS);

        } catch (Exception e) {
            getLogger().error("Error querying documents from Couchbase", e);
            session.transfer(flowFile, FAILURE);
        }
    }
    

    @OnStopped
    public void OnStopped(ProcessContext context) {
        cluster.disconnect();
    }
}
