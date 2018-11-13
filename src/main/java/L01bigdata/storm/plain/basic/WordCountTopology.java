package L01bigdata.storm.plain.basic;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class WordCountTopology {
	
	public static class TrySpout1 implements ISpout{

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			
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
		public void nextTuple() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void ack(Object msgId) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void fail(Object msgId) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
    public static class TrySpout2 extends BaseRichSpout{

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public static class TrySpout3 implements IRichSpout{

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			
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
		public void nextTuple() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void ack(Object msgId) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void fail(Object msgId) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

	
	
	public static class TryBolt1 implements IRichBolt{

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}

	
	public static class TryBolt2 implements IBasicBolt{

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void cleanup() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	
	public static class TryBolt3 extends BaseBasicBolt{

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public static class TryBolt4 extends BaseRichBolt{

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	// Spout的最顶层抽象是ISpout接口
	// 通常情况下（Shell和事务型的除外），实现一个Spout，
	// 可以直接实现接口IRichSpout，如果不想写多余的代码，可以直接继承BaseRichSpout。
	public static class SentenceSpout extends BaseRichSpout {

		private SpoutOutputCollector collector;
		private String[] sentences = { "my dog has fleas",
				"i like cold beverages", "the dog ate my homework",
				"don't have a cow man", "i don't think i like fleas" };
		private int index = 0;

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sentence"));
		}

		@Override
		public void open(Map config, TopologyContext context,
				SpoutOutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void nextTuple() {
			this.collector.emit(new Values(sentences[index]));
			index++;
			if (index >= sentences.length) {
				index = 0;
			}
			Utils.sleep(2000);
		}
	}

	// 继承BaseBasicBolt或BaseRichBolt
	// 前者会自动进行ack或fail，后者需要手工进行，并锚定。

	// 通常情况下，实现一个Bolt，可以实现IRichBolt接口或继承BaseRichBolt，
	// 如果不想自己处理结果反馈，可以实现IBasicBolt接口或继承BaseBasicBolt，
	// 它实际上相当于自动做掉了prepare方法和collector.emit.ack(inputTuple)；
	public static class SplitSentenceBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			String sentence = input.getStringByField("sentence");
			String[] words = sentence.split(" ");
			// System.out.println("---split---"+Thread.currentThread().getName()+"---split-----");
			for (String word : words) {
				collector.emit(new Values(word));
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("word"));
		}

	}

	public static class WordCountBolt extends BaseBasicBolt {

		private HashMap<String, Long> counts = new HashMap<String, Long>();

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			String word = input.getStringByField("word");
			Long count = this.counts.get(word);
			if (count == null) {
				count = 0L;
			}
			count++;
			this.counts.put(word, count);

			System.out.println("-------BEGIN-----------");
			System.out.println(Thread.currentThread().getName() + "  " + word);
			for (String key : counts.keySet())
				System.out.println("word= " + key + " count= "
						+ counts.get(key));
			System.out.println("-------END-----------");
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
	private static final String SPLIT_BOLT_ID = "split-bolt";
	private static final String COUNT_BOLT_ID = "count-bolt";
	private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) throws Exception {
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout(SENTENCE_SPOUT_ID, spout);

		// 2代表本bolt由两个线程（executor）并发执行，默认情况下一个线程执行一个task
		builder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).shuffleGrouping(
				SENTENCE_SPOUT_ID);

		builder.setBolt(COUNT_BOLT_ID, countBolt, 1).fieldsGrouping(
				SPLIT_BOLT_ID, new Fields("word"));

		Config config = new Config();

		if (args.length > 0) {
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config,
					builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, config,
					builder.createTopology());

		}

	}
}