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
import org.apache.nifi.processor.ProcessSessionFactory;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadPoolExecutor;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


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

    public static final PropertyDescriptor ORDER_BY = new PropertyDescriptor.Builder()
        .name("Order By")
        .description("The field to order the documents by")
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
            CONNECTION_NAME, USERNAME, PASSWORD, BUCKET, SCOPE, COLLECTION, ORDER_BY, BATCH_SIZE_MB
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
        ExecutorService executorService = Executors.newFixedThreadPool(3); // 4 threads

        try {
            String orderBy = context.getProperty(ORDER_BY).getValue();
            Runtime runtime = Runtime.getRuntime();
    
            long totalMemory = runtime.totalMemory();
            long maxMemory = runtime.maxMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
    
            getLogger().info("Total memory: " + totalMemory);
            getLogger().info("Max memory: " + maxMemory);
            getLogger().info("Free memory: " + freeMemory);
            getLogger().info("Used memory: " + usedMemory);
    
            long averageSize = 8 * 500;
            QueryResult countOfDocuments = cluster.query("SELECT COUNT(*) FROM `" + bucket.name() + "`.`" + collection.scopeName() + "`.`" + collection.name() + "`");
            long count = countOfDocuments.rowsAsObject().get(0).getLong("$1");
    
            long start = System.currentTimeMillis();
    
            long byteLimit = 8 * 1024 * 1024 * Long.parseLong(context.getProperty(BATCH_SIZE_MB).getValue());
            long batchSize = byteLimit / averageSize;
    
            getLogger().info("Batch size: " + batchSize);
            getLogger().info("Total number of documents: " + count);
            getLogger().info("Average document size: " + averageSize);
            getLogger().info("Total number of batches: " + (count / batchSize + 1));
    
            long totalBatches = count / batchSize + 1;
    
            for (long i = 0; i < totalBatches; i++) {
                long currentOffset = i * batchSize;
                long currentLimit = batchSize;
    
                executorService.submit(() -> {
                    try {
                        getLogger().info("Querying batch: " + (currentOffset / batchSize + 1));
                        long startBatch = System.currentTimeMillis();
    
                        String query = "SELECT Meta().id as COUCHBASE_META_DOCUMENT_ID,`" + collection.name() + "`.* FROM `" + bucket.name() + "`.`" + collection.scopeName() + "`.`" + collection.name() + "` `" + collection.name() + "` ORDER BY " + orderBy + " LIMIT " + currentLimit + " OFFSET " + currentOffset;
                        QueryResult result = cluster.query(query);
    
                        String resultContentString = result.rowsAsObject().toString();
                        resultContentString = resultContentString.substring(1, resultContentString.length() - 1);
                        final String processedResult = resultContentString;
    
                        FlowFile batchFlowFile = session.create();
                        batchFlowFile = session.append(batchFlowFile, out -> out.write("[".getBytes(StandardCharsets.UTF_8)));
                        batchFlowFile = session.append(batchFlowFile, out -> out.write(processedResult.getBytes(StandardCharsets.UTF_8)));
                        batchFlowFile = session.append(batchFlowFile, out -> out.write("]".getBytes(StandardCharsets.UTF_8)));
                        session.transfer(batchFlowFile, SUCCESS);
                        session.commit();
    
                        getLogger().info("Time taken to query batch: " + (System.currentTimeMillis() - startBatch) + "ms");
                    } catch (Exception e) {
                        getLogger().error("Error querying batch from Couchbase", e.getMessage());
                    }
                });
            }
    
            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    
            long end = System.currentTimeMillis();
            getLogger().info("Time taken to query documents from Couchbase: " + (end - start) + "ms");
    
        } catch (Exception e) {
            getLogger().error("Error querying documents from Couchbase", e);
    
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, out -> out.write("[]".getBytes(StandardCharsets.UTF_8)));
    
            session.transfer(flowFile, FAILURE);
        }
    }
    

    @OnStopped
    public void OnStopped(ProcessContext context) {
        cluster.disconnect();
    }
}
