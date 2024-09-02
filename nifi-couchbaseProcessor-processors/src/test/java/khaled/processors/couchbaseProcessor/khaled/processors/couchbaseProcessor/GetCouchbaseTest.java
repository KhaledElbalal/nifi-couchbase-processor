package khaled.processors.couchbaseProcessor;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


@Testcontainers
class GetCouchbaseTest {
    private static final BucketDefinition bucketDefinition = new BucketDefinition("travel-sample");

    @Container
    private static final CouchbaseContainer container = new CouchbaseContainer(DockerImageName.parse("couchbase/server:7.6.2"))
            .withBucket(bucketDefinition);

    private static Collection collection;
    private TestRunner testRunner;

    @BeforeAll
    public static void setupCluster() {
        Cluster cluster = Cluster.connect(container.getConnectionString(),
                container.getUsername(),
                container.getPassword());
        Bucket bucket = cluster.bucket("travel-sample");
        collection = bucket.defaultCollection();
    }

    @BeforeEach
    void setup() {
        testRunner = TestRunners.newTestRunner(GetCouchbase.class);
        testRunner.setProperty(GetCouchbase.CONNECTION_NAME, container.getConnectionString());
        testRunner.setProperty(GetCouchbase.USERNAME, container.getUsername());
        testRunner.setProperty(GetCouchbase.PASSWORD, container.getPassword());
        testRunner.setProperty(GetCouchbase.BUCKET, "travel-sample");
        testRunner.setProperty(GetCouchbase.SCOPE, "_default");
        testRunner.setProperty(GetCouchbase.COLLECTION, "_default");
        testRunner.setProperty(GetCouchbase.ORDER_BY, "name");
        testRunner.setProperty(GetCouchbase.BATCH_SIZE_MB, "1");
    }

    @Test
    void testProcessorInitialization() {
        // Test if the processor initializes correctly with given properties
        testRunner.assertValid();
    }

    @Test
    void testCouchbaseQueryAndFlowFileCreation() {
        // Insert a document into the Couchbase collection
        JsonObject doc = JsonObject.create().put("name", "Test Document").put("type", "test");
        MutationResult insertResult = collection.insert("test-doc-1", doc);
        assertNotNull(insertResult, "Document should be inserted successfully");

        // Run the processor
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // Validate success relationship and FlowFile content
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(GetCouchbase.SUCCESS);
        assertEquals(1, successFiles.size(), "One FlowFile should be transferred to success");

        MockFlowFile flowFile = successFiles.getFirst();
        flowFile.assertContentEquals("[{\"name\":\"Test Document\",\"type\":\"test\",\"COUCHBASE_META_DOCUMENT_ID\":\"test-doc-1\"}]");

        // Cleanup: Remove the document
        MutationResult removeResult = collection.remove("test-doc-1");
    }

    @Test
    void testEmptyCollectionProcessing() {
        // Run the processor without inserting any documents
        testRunner.enqueue(new byte[0]);
        testRunner.run();

        // Validate success relationship and FlowFile content for empty collection
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(GetCouchbase.SUCCESS);
        assertEquals(1, successFiles.size(), "One FlowFile should be transferred to success");

        MockFlowFile flowFile = successFiles.getFirst();
        flowFile.assertContentEquals("[]");
    }

    @Test
    void testProcessorFailureHandling() {
        // Provide an incorrect bucket name to trigger a failure
        testRunner.setProperty(GetCouchbase.BUCKET, "non-existent-bucket");

        // Expect an AssertionError when asserting the processor's validity with an incorrect bucket name
        assertThrows(AssertionError.class, () -> testRunner.assertValid(),
                "Processor should not be valid with incorrect bucket name");
    }

    @Test
    void testRelationships() {
        // Test if the processor has the correct relationships
        assertEquals(2, testRunner.getProcessor().getRelationships().size(), "Processor should have two relationships");
        assertTrue(testRunner.getProcessor().getRelationships().contains(GetCouchbase.SUCCESS), "Processor should have a success relationship");
        assertTrue(testRunner.getProcessor().getRelationships().contains(GetCouchbase.FAILURE), "Processor should have a failure relationship");
    }

    @Test
    void testMissingConnectionName() {
        testRunner.removeProperty(GetCouchbase.CONNECTION_NAME);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without a connection name");
    }

    @Test
    void testMissingUsername() {
        testRunner.removeProperty(GetCouchbase.USERNAME);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without a username");
    }

    @Test
    void testMissingPassword() {
        testRunner.removeProperty(GetCouchbase.PASSWORD);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without a password");
    }

    @Test
    void testMissingBucket() {
        testRunner.removeProperty(GetCouchbase.BUCKET);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without a bucket");
    }

    @Test
    void testMissingScope() {
        testRunner.removeProperty(GetCouchbase.SCOPE);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without a scope");
    }

    @Test
    void testMissingCollection() {
        testRunner.removeProperty(GetCouchbase.COLLECTION);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without a collection");
    }

    @Test
    void testMissingOrderBy() {
        testRunner.removeProperty(GetCouchbase.ORDER_BY);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without an order by field");
    }

    @Test
    void testMissingBatchSize() {
        testRunner.removeProperty(GetCouchbase.BATCH_SIZE_MB);
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid without a batch size");
    }

    @Test
    void testInvalidBatchSize() {
        testRunner.setProperty(GetCouchbase.BATCH_SIZE_MB, "invalid");
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid with an invalid batch size");
    }

    @Test
    void testNegativeBatchSize() {
        testRunner.setProperty(GetCouchbase.BATCH_SIZE_MB, "-1");
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid with a negative batch size");
    }

    @Test
    void testZeroBatchSize() {
        testRunner.setProperty(GetCouchbase.BATCH_SIZE_MB, "0");
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid with a zero batch size");
    }

    @Test
    void testEmptyBatchSize() {
        testRunner.setProperty(GetCouchbase.BATCH_SIZE_MB, "");
        assertThrows(AssertionError.class, () -> testRunner.assertValid(), "Processor should be invalid with an empty batch size");
    }
}
