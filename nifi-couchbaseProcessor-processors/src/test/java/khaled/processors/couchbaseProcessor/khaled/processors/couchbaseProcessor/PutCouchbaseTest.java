import khaled.processors.couchbaseProcessor.PutCouchbase;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class PutCouchbaseTest {

    private TestRunner testRunner;

    @BeforeEach
    public void init() {
        testRunner = TestRunners.newTestRunner(PutCouchbase.class);
        testRunner.setProperty(PutCouchbase.CONNECTION_NAME, "localhost");
        testRunner.setProperty(PutCouchbase.USERNAME, "admin");
        testRunner.setProperty(PutCouchbase.PASSWORD, "password");
        testRunner.setProperty(PutCouchbase.BUCKET, "default");
        testRunner.setProperty(PutCouchbase.SCOPE, "_default");
        testRunner.setProperty(PutCouchbase.COLLECTION, "_default");
    }

    @Test
    void testProcessorInvalidBucketName() {
        // Set an invalid bucket name to trigger a failure
        testRunner.setProperty(PutCouchbase.BUCKET, "non-existent-bucket");

        // Expect an AssertionError when asserting the processor's validity
        assertThrows(AssertionError.class, () -> testRunner.assertValid(),
                "Processor should not be valid with incorrect bucket name");
    }

    @Test
    void testProcessorInvalidUsername() {
        // Set an invalid username to trigger a failure
        testRunner.setProperty(PutCouchbase.USERNAME, "invalid-user");

        // Expect an AssertionError when asserting the processor's validity
        assertThrows(AssertionError.class, () -> testRunner.assertValid(),
                "Processor should not be valid with incorrect username");
    }

    @Test
    void testProcessorInvalidPassword() {
        // Set an invalid password to trigger a failure
        testRunner.setProperty(PutCouchbase.PASSWORD, "invalid-password");

        // Expect an AssertionError when asserting the processor's validity
        assertThrows(AssertionError.class, () -> testRunner.assertValid(),
                "Processor should not be valid with incorrect password");
    }

    @Test
    void testProcessorInvalidScopeName() {
        // Set an invalid scope name to trigger a failure
        testRunner.setProperty(PutCouchbase.SCOPE, "non-existent-scope");

        // Expect an AssertionError when asserting the processor's validity
        assertThrows(AssertionError.class, () -> testRunner.assertValid(),
                "Processor should not be valid with incorrect scope name");
    }

    @Test
    void testProcessorInvalidCollectionName() {
        // Set an invalid collection name to trigger a failure
        testRunner.setProperty(PutCouchbase.COLLECTION, "non-existent-collection");

        // Expect an AssertionError when asserting the processor's validity
        assertThrows(AssertionError.class, () -> testRunner.assertValid(),
                "Processor should not be valid with incorrect collection name");
    }
}
