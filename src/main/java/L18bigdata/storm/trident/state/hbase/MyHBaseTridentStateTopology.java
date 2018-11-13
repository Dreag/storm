package L18bigdata.storm.trident.state.hbase;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MyHBaseTridentStateTopology {
	private static TridentState tridentStreamToHbase(TridentTopology topology,
			FixedBatchSpout spout) {
		MyHbaseState.Options options = new MyHbaseState.Options();
		options.setTableName("storm_trident_state");
		options.setColumFamily("colum1");
		options.setQualifier("q1");
		/**
		 * 根据数据源拆分单词后,然后分区操作,在每个分区上又进行分组(hash算法),然后在每个分组上进行聚合
		 * 所以这里可能有多个分区,每个分区有多个分组,然后在多个分组上进行聚合
		 * 用来进行group的字段会以key的形式存在于State当中，聚合后的结果会以value的形式存储在State当中
		 */
		return topology
				.newStream("sentencestream", spout)
				.each(new Fields("sentence"), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.persistentAggregate(new MyHbaseState.HbaseFactory(options),
						new Count(), new Fields("count")).parallelismHint(3);
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException, AuthorizationException {
		TridentTopology topology = new TridentTopology();
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
				new Values("tanjie is a good man"), new Values(
						"what is your name"), new Values("how old are you"),
				new Values("my name is tanjie"), new Values("i am 18"));
		spout.setCycle(false);
		tridentStreamToHbase(topology, spout);
		Config config = new Config();
		config.setDebug(false);
		StormSubmitter
				.submitTopologyWithProgressBar(
						"word_count_trident_state_HbaseState", config,
						topology.build());
	}

}
