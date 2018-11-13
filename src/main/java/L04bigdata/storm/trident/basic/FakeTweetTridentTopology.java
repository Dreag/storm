package L04bigdata.storm.trident.basic;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

//发出推文，形如（文本，国家）
//1.首先过滤掉文本中不含FIFA的。
//2.其次对属于同一批的不同partition的tuple归拢到同一partition，按国家分组
//3.统计各组中tuple的数量。

public class FakeTweetTridentTopology {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Count", conf, buildTopology());
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
	
	public static StormTopology buildTopology() {

		FakeTweetSpout spout = new FakeTweetSpout(10);
		
		TridentTopology topology = new TridentTopology();

		topology.newStream("spout1", spout)
				.shuffle()
				.each(new Fields("text", "Country"),
						new FakeTweetTridentUtility.TweetFilter())
				.groupBy(new Fields("Country"))
				.aggregate(new Fields("Country"), new FakeTweetTridentUtility.TweetCount(),
						new Fields("country","count"))
				.each(new Fields("country","count"), new FakeTweetTridentUtility.Print())
				.parallelismHint(2);
		

		return topology.build();
	}
}
