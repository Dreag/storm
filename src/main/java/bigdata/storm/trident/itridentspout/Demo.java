package bigdata.storm.trident.itridentspout;

import java.net.InetSocketAddress;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import L18bigdata.storm.trident.state.redis.RedisStateFactory;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class Demo {

	public static StormTopology buildTopology(StateFactory stateFactory,LocalDRPC drpc) {
		// A Trident topology operates in a higher level than a Storm topology.
		TridentTopology topology = new TridentTopology();

		// Create a simple new stream using the `RepeatWordsSpout`. In this
		// topology,
		// we group the words emitted by `RepeatWordsSpout` by the field and
		// store the count in
		// `StateFactory` passed to the function. In order to aggregate across
		// the batches
		// We use persistentAggregate function. parallelismHint provides a hint
		// for the
		// parallelism of the topology.
		TridentState wordCounts = topology
				.newStream("wordCount", new RepeatWordsSpout())
				.parallelismHint(1)
				.groupBy(new Fields("word"))
				.persistentAggregate(stateFactory, new Count(),
						new Fields("count")).parallelismHint(1);

		// DRPC stands for Distributed Remote Procedure Call.
		// We can issue calls to the following DRPC Stream using a client
		// library.
		// A DRPC call takes two Strings, function name and function arguments.
		// Function arguments are available as the field `args`.
		// In the following DRPC Stream defined with the name "words", we split
		// the words
		// contained in the `args` and return the count of each word currently
		// stored
		// in the state.
		topology.newDRPCStream("words",drpc)
				.parallelismHint(1)
				.each(new Fields("args"), new Split(), new Fields("word"))
				.parallelismHint(1)
				.groupBy(new Fields("word"))
				.stateQuery(wordCounts, new Fields("word"), new MapGet(),
						new Fields("count")).parallelismHint(1)
				.each(new Fields("count"), new FilterNull()).parallelismHint(1);

		return topology.build();
	}

	public static void main(String[] args) throws Exception {
		// We create the redis StateFactory using the `trident-redis` library.
		// We use redis to store our state.
		StateFactory redis = MyRedisStateFactory
				.transactional(new InetSocketAddress("localhost", 6379));

		LocalDRPC drpc = new LocalDRPC();
		
		// Build the topology
		StormTopology topology = buildTopology(redis,drpc);

		// `conf` stores the configuration information that needs to be supplied
		// with
		// the strom topology.
		Config conf = new Config();
		conf.setDebug(false);

		// Run locally if no input arguments are present.
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("tester", conf, topology);
		
		
		
		
		
//		try {
//			for (int i = 0; i < 100; i++) {
//				System.out.println("DRPC: " + drpc.execute("words", "apple ball cat dog"));
//				Utils.sleep(1000);
//			}
//			
//		} catch (JedisConnectionException e) {
//			throw new RuntimeException("Unfortunately, this test requires redis-server runing on localhost:6379", e);
//		}


	}
}
