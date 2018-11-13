package bigdata.storm.trident.itridentspout;

import org.apache.storm.tuple.Values;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.trident.topology.TransactionAttempt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RWEmitter implements ITridentSpout.Emitter<List<Long>>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final long N = 10000;

    // We store two type of batches to be emitted based on the mod value of the transaction id.
    List<List<Object>> batchA = new ArrayList<List<Object>>() {{
        add(new Values("apple"));
        add(new Values("ball"));
        add(new Values("cat"));
        add(new Values("dog"));
    }};

    List<List<Object>> batchB = new ArrayList<List<Object>>() {{
        add(new Values("a"));
        add(new Values("b"));
        add(new Values("c"));
        add(new Values("d"));
    }};

    private static final Logger LOG = LoggerFactory.getLogger(RWEmitter.class);

    // Emit a batch based on the transactionAttempt and the metadata (which was created
    // in the `initializeTransaction` function. Read more about the interface functions in the following link:
    // https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/spout/ITridentSpout.java#L58
    @Override
    public void emitBatch(TransactionAttempt transactionAttempt, List<Long> coordinatorMeta, TridentCollector tridentCollector) {
        List<List<Object>> batch;

        // Check whether we need to emit more batches to reach .
        if (coordinatorMeta.get(0) < N) {
            LOG.info("Emitting transaction [" + transactionAttempt.getTransactionId() + "] " + coordinatorMeta);
            System.out.println("Emitting transaction [" + transactionAttempt.getTransactionId() + "] " + coordinatorMeta);
            // If the stored mod value is 1 we emit the batchA otherwise batchB.
            if (coordinatorMeta.get(1) == 0L) {
                batch = batchA;
            } else {
                batch = batchB;
            }

            // emit the Batch
            for (List<Object> tuple : batch) {
                tridentCollector.emit(tuple);
            }
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void success(TransactionAttempt transactionAttempt) {
    }

    @Override
    public void close() {
    }
}
