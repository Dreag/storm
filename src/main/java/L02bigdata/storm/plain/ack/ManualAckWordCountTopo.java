package L02bigdata.storm.plain.ack;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

//手工写代码实现ack和fail
//
//注意spout实现IRichSpout借口，bolt继承BaseRichBolt。
public class ManualAckWordCountTopo {

	public static class ManualAckWordReaderSpout implements IRichSpout {

		SpoutOutputCollector collector;
		int index;
//		String[] sentences = { "1 one", "2 two", "3 three", "4 four", "5 five",
//				"6 six" };
		String[] sentences = { "1 one" };
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
			index = 0;

		}



		@Override
		public void nextTuple() {
			 Utils.sleep(2000);
			 if (index < sentences.length)
			 collector.emit(new Values(sentences[index]), 66);
			 else {
			
			 }
			 index++;
		}

		@Override
		public void ack(Object msgId) {
			// TODO Auto-generated method stub
			System.out.println("message" + msgId + " "
					+ "has been done succussfully.");
		}

		@Override
		public void fail(Object msgId) {
			// TODO Auto-generated method stub
			System.out.println("message" + msgId + " "
					+ "failed,and resending now...");
			collector.emit(new Values(sentences[(Integer) 0]),
					(Integer) msgId);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("sentence"));
		}
		
		@Override
		public void close() {
			// TODO Auto-generated method stub
		}

		@Override
		public void activate() {
			// TODO Auto-generated method stub
		}

		@Override
		public void deactivate() {
			// TODO Auto-generated method stub
		}
		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

	}

	// BaseRichBolt需要手工ack和fail
	public static class ManualAckWordSpliterBolt extends BaseRichBolt {

		OutputCollector collector;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
		}
		
		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String sentence = input.getString(0);
			String[] words = sentence.split(" ");
			for (String w : words) {
				w = w.trim();
				if (!w.isEmpty())
					collector.emit(input, new Values(w));// anchoring
			}

			collector.ack(input);
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("word"));
		}

	}

	public static class ManualAckWordWriterBolt extends BaseRichBolt {

		private OutputCollector collector;
		private FileWriter fw;

		private int total = 0;

		// 以下三个方法必须覆盖。
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			this.collector = collector;
			try {
				fw = new FileWriter("d:/result.txt");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String word = input.getString(0);
			if (total == 5)
				collector.fail(input);// 要求立即重传，不用等超时。
			else {
				try {
					fw.write(word);
					fw.write("\n");
					fw.flush();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				 collector.emit(input,new Values(word));
				collector.ack(input); // 此处的ack调用并不一定会导致spout中的ack被调用。
										// 只有“此处input且和该input属于同一个tuple树的其他所有tuple”都被成功处理，才会导致spout总的ack被调用。
										// 如在此例中，input为3和input为three是来自同一个树的不同的tuple，虽然3导致此处ack调用（此处total为4）
										// 但不会导致spout中的ack调用，只有当three也调用了此处的ack，才会导致spout的ack调用。
			}
			total++;
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("word", "count"));
		}

	}

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new ManualAckWordReaderSpout(), 1);
		builder.setBolt("bolt_spliter", new ManualAckWordSpliterBolt(), 1)
				.shuffleGrouping("input");
		builder.setBolt("bolt_count", new ManualAckWordWriterBolt(), 1)
				.fieldsGrouping("bolt_spliter", new Fields("word"));

		Config config = new Config();

		if (args.length > 0) {
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			System.out.println("start localcluster.");
			cluster.submitTopology("wordcount", config,
					builder.createTopology());
		}

	}
}