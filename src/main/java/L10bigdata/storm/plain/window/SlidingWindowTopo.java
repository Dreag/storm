package L10bigdata.storm.plain.window;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class SlidingWindowTopo {

	public static class NumberSpout extends BaseRichSpout {

		private static final long serialVersionUID = 1L;

		private SpoutOutputCollector collector;

		int intsmaze = 0;

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("intsmaze"));
		}

		public void nextTuple() {
			// System.out.println("发送数据:" + intsmaze);
			collector.emit(new Values(intsmaze++));
			try {
				Thread.sleep(2000);
				// Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;

		}
	}

	public static class SlidingWindowBolt extends BaseWindowedBolt {

		private static final long serialVersionUID = 1L;
		private OutputCollector collector;

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void execute(TupleWindow inputWindow) {
			int sum = 0;
			
			System.out.print("一个窗口内的数据");
			for (Tuple tuple : inputWindow.get()) {
				// get是获取窗口此时的全部数据。
				// getNew是获取本次新进入窗口的数据。
				// getExpired获取上次在窗口，本次已移出窗口的数据。
				//滚动窗口只有get才有意义。
				int str = (Integer) tuple.getValueByField("intsmaze");
				System.out.print(" " + str);
				sum += str;
			}
			System.out.println("=======" + sum); //计算和。
			// collector.emit(new Values(sum));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// declarer.declare(new Fields("count"));
		}

	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NumberSpout(), 1);
		// builder.setBolt("slidingwindowbolt", new SlidingWindowBolt()
		// .withWindow(new Count(6), new Count(2)),1)
		// .shuffleGrouping("spout");
		// 滑窗 窗口长度：tuple数, 滑动间隔: tuple数 每收到2条数据统计当前6条数据的总和。

		builder.setBolt(
				"bolt",
				new SlidingWindowBolt().withWindow(new Duration(10,
						TimeUnit.SECONDS), new Duration(5, TimeUnit.SECONDS)),
				1).shuffleGrouping("spout");// 每5秒统计最近10秒的数据

		Config config = new Config();

		if (args.length > 0) {
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("sliding", config, builder.createTopology());

		}
	}

}