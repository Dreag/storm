package bigdata.storm.trident.itridentspout;

import org.apache.storm.trident.spout.ITridentSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class RWBatchCoordinator implements ITridentSpout.BatchCoordinator<List<Long>>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RWBatchCoordinator.class);

    // `initializeTransaction` method is responsible for creating metadata for a transaction.
    // This metadata should be capable of replaying the exact batch in case of a failure (since
    // we are using a transient spout). Created metadata is stored in zookeeper and used
    // when replaying the batches.
    //
    // `currMetaData` is the metadata for the current batch and `prevMetaData` is the metadata for the
    // previous batch. We need to update and return `currMetaData` based on `prevMetaData` in our
    // demonstration.
    //
    // In our demonstration we store a count and the mod of the transaction id by 2. The stored mod
    // value helps to select the right batch to replay and the stored count helps to decide when to
    // stop emitting the batches. Read more about this function and other interface functions in the following link:
    // https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/spout/ITridentSpout.java#L29
    @Override
    public List<Long> initializeTransaction(final long txid, final List<Long> prevMetaData, List<Long> currMetaData) {
        LOG.info("Initializing transaction [" + txid + "]");

        if (prevMetaData == null && currMetaData == null) {
            // This is the very first batch, we initilize it with a count 0 and the mod
            // value of the transaction
            currMetaData = new ArrayList<Long>() {{add(0L); add(txid % 2);}};
        } else if (currMetaData == null) {
            // This is the first time the batch with transaction id `txid` is going to be
            // emitted. We update `currMetaData` based on previous metadata.
            currMetaData = new ArrayList<Long>() {{add(prevMetaData.get(0) + 1); add(txid % 2);}};
        }
        return currMetaData;
    }

    @Override
    public void success(long txid) {
        LOG.info("Successful transaction [" + txid + "]");
    }

    @Override
    public boolean isReady(long l) {
        return true;
    }

    @Override
    public void close() {
    }
}