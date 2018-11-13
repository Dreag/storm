package L09bigdata.storm.plain.drpc.remote;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RemoteDRPCTopology {
	public static class ExclamationBolt extends BaseBasicBolt {

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("result", "return-info"));
		}

		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String arg = tuple.getString(0);
			Object retInfo = tuple.getValue(1);
			collector.emit(new Values(arg + "!!!", retInfo));
		}

	}

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();
		// 开始的Spout
		DRPCSpout drpcSpout = new DRPCSpout("function");
		builder.setSpout("drpc-input", drpcSpout, 5);

		// 真正处理的Bolt
		builder.setBolt("exclaim", new ExclamationBolt(), 5).noneGrouping(
				"drpc-input");

		// 结束的ReturnResults
		builder.setBolt("return", new ReturnResults(), 5).noneGrouping(
				"exclaim");

		Config conf = new Config();
		conf.setDebug(false);
		conf.setMaxTaskParallelism(3);

		try {
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} catch (AlreadyAliveException | InvalidTopologyException
				| AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
