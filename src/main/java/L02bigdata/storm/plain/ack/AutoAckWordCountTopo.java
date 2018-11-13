package L02bigdata.storm.plain.ack;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


//处理成功自动调用spout的ack。
//处理失败，自动调用spout的fail。
//但无法在代码里人工调用fail表达“失败”
//只能利用debug暂停的机制模拟后一个bolt收不到tuple的情况。
public class AutoAckWordCountTopo {
	
	public static class AutoAckWordReaderSpout extends BaseRichSpout {

		SpoutOutputCollector collector;
		int index;
		String[] sentences = { "1 one", "2 two", "3 three", "4 four", "5 five",
				"6 six" };

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			index = 0;
			this.collector = collector;
		}


		@Override
		public void nextTuple() {
			// TODO Auto-generated method stub
			Utils.sleep(2000);
			if (index < sentences.length)
				collector.emit(new Values(sentences[index]),index);//不添加index的话就不支持可靠性保证。
			else {

			}
			index++;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("sentence"));
		}

		// ack方法和fail方法是为了测试显示效果人为强制覆盖添加的。
		public void ack(Object msgId) {
			System.out.println("message" + msgId + " "
					+ "has been done succussfully.");
		}

		public void fail(Object msgId) { // TODO Auto-generated method stub
			System.out.println("message" + msgId + " "
					+ "failed,and resending now...");
			collector.emit(new Values(sentences[(Integer) msgId]), (Integer) msgId);
		}

	}
	
	public static class AutoAckWordSpliterBolt extends BaseBasicBolt {
		//本类中的两个方法是必须写的。
		
		
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			String sentence = input.getString(0);
			String[] words = sentence.split(" ");
			System.out.println(input);
			for (String w : words) {
				w = w.trim();
				if (!w.isEmpty())
					collector.emit(new Values(w));////此处已自动anchoring
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("word")); 
		}

	}
	
	public static class AutoAckWordWriterBolt extends BaseBasicBolt {


		private FileWriter fw;

		//BaseBasicBolt的子类可以没有该方法，此处特地写出来，
		//是为了进行fw的实例化。
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				//初始化必须在prepare里，而不是构造方法里，会报错。
				fw = new FileWriter("d:/stormout.txt");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/* 这样实例化fw会报错。
		public WordWriter() {
			try {
				fw = new FileWriter("d:/stormout.txt");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			total = 0;
		}
		
		*/

		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			String word = input.getString(0);
				try {
					fw.write(word);
					fw.write("\n");
					fw.flush();
				} catch (IOException e) {
				}

		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}
	
	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("input", new AutoAckWordReaderSpout(), 1);
		builder.setBolt("bolt_spliter", new AutoAckWordSpliterBolt(), 1).shuffleGrouping("input");
		builder.setBolt("bolt_count", new AutoAckWordWriterBolt(), 1).fieldsGrouping("bolt_spliter", new Fields("word"));
		
		Config config = new Config();
		
		if(args.length>0){
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		}
		else{
			LocalCluster cluster = new LocalCluster();
			System.out.println("start localcluster.");
			cluster.submitTopology("wordcount", config, builder.createTopology());
		}
		
	}
}