package khaled.processors.couchbaseProcessor;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Scope;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessorInitializationContext;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractCouchbaseProcessor extends AbstractProcessor {
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successful processing")
            .build();
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed processing")
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
            .sensitive(true)
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

    protected Cluster cluster;

    private final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(SUCCESS, FAILURE)));

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        cluster = Cluster.connect(
                context.getProperty(CONNECTION_NAME).getValue(),
                context.getProperty(USERNAME).getValue(),
                context.getProperty(PASSWORD).getValue()
        );

        // Verify that the bucket, scope, and collection exist
        final Bucket bucket = cluster.bucket(context.getProperty(BUCKET).getValue());
        bucket.waitUntilReady(Duration.ofSeconds(10));
        final Scope scope = bucket.scope(context.getProperty(SCOPE).getValue());
        scope.collection(context.getProperty(COLLECTION).getValue());
    }

    @OnStopped
    public void onStopped() {
        if (cluster != null) {
            cluster.disconnect();
        }
    }
}
