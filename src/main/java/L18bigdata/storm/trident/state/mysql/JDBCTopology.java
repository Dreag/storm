package L18bigdata.storm.trident.state.mysql;

import java.util.Arrays;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * 事物Trident-MySQL Topology
 * @author mengyao
 *
 */
@SuppressWarnings("all")
public class JDBCTopology {

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        
        //Spout数据源
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("tels"), 7, 
                new Values("189111    3"),
                new Values("135111    7"),
                new Values("189111    2"),
                new Values("158111    5"),
                new Values("159111    6"),
                new Values("159111    3"),
                new Values("158111    5")
                );
        spout.setCycle(false);
        
        //State持久化配置属性
        JDBCStateConfig config = new JDBCStateConfig();
        config.setDriver("com.mysql.jdbc.Driver");
        config.setUrl("jdbc:mysql://localhost:3306/test");
        config.setUsername("root");
        config.setPassword("123456");
        config.setBatchSize(10);
        config.setCacheSize(10);
        config.setType(StateType.TRANSACTIONAL);
        config.setCols("tel");
        config.setColVals("sum");
        config.setTable("tbl_tel");
        
        topology.newStream("spout", spout)
            .each(new Fields("tels"), new KeyValueFun(), new Fields("tel", "money"))
            .groupBy(new Fields("tel"))
            .persistentAggregate(JDBCState.getFactory(config), new Fields("money"), new SumCombinerAgg(), new Fields("sum"));
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test1", new Config(), topology.build());
    }

}

@SuppressWarnings("all")
class KeyValueFun extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String record = tuple.getString(0);
        collector.emit(new Values(record.split("\t")[0], record.split("\t")[1]));
    }
}

@SuppressWarnings("all")
class SumCombinerAgg implements CombinerAggregator<Long> {
    @Override
    public Long init(TridentTuple tuple) {
        return Long.parseLong(tuple.getString(0));
    }
    @Override
    public Long combine(Long val1, Long val2) {
        Long val = val1+val2;
        System.out.println(val);
        return val;
    }
    @Override
    public Long zero() {
        return 0L;
    }
}
