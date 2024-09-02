package khaled.processors.couchbaseProcessor;

import com.couchbase.client.java.Cluster;

import com.couchbase.client.java.query.QueryResult;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


@Tags({"couchbase", "query", "document"})
@CapabilityDescription("Queries all documents from a Couchbase collection and writes the results to a FlowFile")
public class GetCouchbase extends AbstractCouchbaseProcessor {
    public static final PropertyDescriptor ORDER_BY = new PropertyDescriptor.Builder()
        .name("Order By")
        .description("The field to order the documents by")
        .defaultValue("Meta().id")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor BATCH_SIZE_MB = new PropertyDescriptor.Builder()
        .name("Batch Size (MB)")
        .description("The size of each batch in MB")
        .defaultValue("100")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .build();

    private final List<PropertyDescriptor> descriptors = Collections.unmodifiableList(Arrays.asList(
        CONNECTION_NAME, USERNAME, PASSWORD, BUCKET, SCOPE, COLLECTION, ORDER_BY, BATCH_SIZE_MB
    ));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            ExecutorService executorService = Executors.newFixedThreadPool(4); // 4 threads
            final ReentrantLock lock = new ReentrantLock();

            final String bucketName = context.getProperty(BUCKET).getValue();
            final String scopeName = context.getProperty(SCOPE).getValue();
            final String collectionName = context.getProperty(COLLECTION).getValue();
            final String orderBy = context.getProperty(ORDER_BY).getValue();
    
            final long averageSize = 8 * 500;
            final QueryResult countOfDocuments = cluster.query("SELECT COUNT(*) FROM `" + bucketName + "`.`" + collectionName + "`.`" + collectionName + "`");
            final long count = countOfDocuments.rowsAsObject().getFirst().getLong("$1");
    
            final long start = System.currentTimeMillis();
    
            final long byteLimit = 8 * 1024 * 1024 * Long.parseLong(context.getProperty(BATCH_SIZE_MB).getValue());
            final long batchSize = byteLimit / averageSize;
    
            getLogger().info("Batch size: " + batchSize);
            getLogger().info("Total number of documents: " + count);
            getLogger().info("Average document size: " + averageSize);
            getLogger().info("Total number of batches: " + (count / batchSize + 1));
    
            final long totalBatches = count / batchSize + 1;
    
            for (long i = 0; i < totalBatches; i++) {
                final long currentOffset = i * batchSize;
                executorService.submit(() -> {
                    try {
                        getBatch(session, currentOffset, batchSize, collectionName, bucketName, orderBy, scopeName, lock);
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
            getLogger().error("Error querying documents from Couchbase", e.getMessage());

            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, out -> out.write("[]".getBytes(StandardCharsets.UTF_8)));

            session.transfer(flowFile, FAILURE);
        }
    }

    private void getBatch(ProcessSession session, long currentOffset, long batchSize, String collectionName, String bucketName, String orderBy, String scopeName, ReentrantLock lock) {
        getLogger().info("Querying batch: " + (currentOffset / batchSize + 1));

        final String query = "SELECT Meta().id as COUCHBASE_META_DOCUMENT_ID,`" + collectionName + "`.* FROM `" + bucketName + "`.`" + scopeName + "`.`" + collectionName + "` `" + collectionName + "` ORDER BY " + orderBy + " LIMIT " + batchSize + " OFFSET " + currentOffset;
        QueryResult result = cluster.query(query);

        String resultContentString = result.rowsAsObject().toString();
        final String processedResult = resultContentString.substring(1, resultContentString.length() - 1);

        lock.lock();
        try {
            FlowFile batchFlowFile = session.create();
            batchFlowFile = session.write(batchFlowFile, out -> {
                out.write("[".getBytes(StandardCharsets.UTF_8));
                out.write(processedResult.getBytes(StandardCharsets.UTF_8));
                out.write("]".getBytes(StandardCharsets.UTF_8));
            });
            session.transfer(batchFlowFile, SUCCESS);
            session.commit();
        } finally {
            lock.unlock();
        }
    }
}