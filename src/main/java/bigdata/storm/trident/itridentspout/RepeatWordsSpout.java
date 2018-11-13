package bigdata.storm.trident.itridentspout;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.spout.ITridentSpout;
import org.apache.storm.tuple.Fields;

import java.util.List;
import java.util.Map;

// Spout class with the Emitter and Coordinator we created. Read more about the interface
// in the following link:
// https://github.com/apache/storm/blob/master/storm-core/src/jvm/storm/trident/spout/ITridentSpout.java
public class RepeatWordsSpout implements ITridentSpout<List<Long>> {
    private static final long serialVersionUID = 1L;
    BatchCoordinator<List<Long>> coordinator = new RWBatchCoordinator();
    Emitter<List<Long>> emitter = new RWEmitter();

    @Override
    public Emitter<List<Long>> getEmitter(String s, Map map, TopologyContext topologyContext) {
        return emitter;
    }

    @Override
    public Map getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("word");
    }

    @Override
    public BatchCoordinator<List<Long>> getCoordinator(String s, Map map, TopologyContext topologyContext) {
        return coordinator;
    }
}
