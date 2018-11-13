package L03bigdata.storm.plain.streamsplit;

import java.util.Map;


//一个上游组件，根据数据所属类不同，将不同的数据发送给不同的下游组件。
//此时引入Stream的概念。
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

//上游组件将”相同“的数据发往下游不同的两个组件
//此时上游组件和惯用写法一致。
//只是在topology中，要在上游组件后面接两个不同的下游组件。

public class WordCountTopologySplit1 {

    public static class RandomSentenceSpout extends BaseRichSpout {

        SpoutOutputCollector collector;
        Random rand;
        String[] sentences =null;

        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.collector = spoutOutputCollector;
            rand = new Random();
            sentences = new String[]{ "A the cow jumped over the moon", "B an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };

        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence"));
        }

        public void nextTuple() {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String sentence = sentences[rand.nextInt(sentences.length)];

            this.collector.emit(new Values(sentence));

        }
    }

    public static class SplitASentenceBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			String sentence = input.getStringByField("sentence");
            System.out.println("SplitA:"+Thread.currentThread().getName()+" "+sentence);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}

    }
    
    public static class SplitBSentenceBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
          String sentence = input.getStringByField("sentence");
          System.out.println("SplitB:"+Thread.currentThread().getName()+" "+sentence);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			
		}

    }


    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new RandomSentenceSpout(), 1);
        
        
        //两个下游组件上接同一个上游组件的同一个流（流id为default）
        builder.setBolt("splitA", new SplitASentenceBolt(), 2).shuffleGrouping("spout");
        builder.setBolt("splitB", new SplitBSentenceBolt(), 2).shuffleGrouping("spout");


        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("word-count", conf, builder.createTopology());
        Thread.sleep(60000);
        cluster.shutdown();

    }
}