package L06bigdata.storm.trident.aggregator.batch;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class ReducerAggregatorTest {

	public static class Debug extends BaseFilter {

		private static final long serialVersionUID = -3136720361960744881L;
		private final String name;
		private int partitionIndex;

		public Debug() {
			this(false);
		}

		public Debug(boolean useLogger) {
			this.name = "DEBUG: ";
		}

		public Debug(String name) {
			this.name = "DEBUG(" + name + "): ";
		}

		@Override
		public void prepare(Map conf, TridentOperationContext context) {
			System.out.println("---------I am prepared."+ Thread.currentThread().getName() );
			this.partitionIndex = context.getPartitionIndex();
			super.prepare(conf, context);
		}

		public boolean isKeep(TridentTuple tuple) {
			System.out.println("<" + new Date() + "[partition" + partitionIndex
					+ "-" + Thread.currentThread().getName() + "]" + "> "
					+ name + tuple.toString());
			return true;
		}
	}

	public static class MyTestSpout implements IBatchSpout {

		private int batchSize;

		public MyTestSpout(int batchSize) {
			this.batchSize = batchSize;
		}

		private static final ArrayList<Values> SOURCES = new ArrayList<Values>();

		static {
			SOURCES.add(new Values("name1", 4));
			SOURCES.add(new Values("name2", 3));
			SOURCES.add(new Values("name1", 5));
			SOURCES.add(new Values("name3", 5));
			SOURCES.add(new Values("name1", 2));
			SOURCES.add(new Values("name2", 7));
			SOURCES.add(new Values("name3", 8));
		}

		public void open(Map conf, TopologyContext context) {
			// TODO Auto-generated method stub

		}

		public void ack(long batchId) {
			// TODO Auto-generated method stub

		}

		public void close() {
			// TODO Auto-generated method stub

		}

		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}

		public Fields getOutputFields() {
			// TODO Auto-generated method stub
			return new Fields("user", "score");
		}

		private List<Object> recordGenerator() {
			final Random rand = new Random();
			int randomNumber = rand.nextInt(5);
			return SOURCES.get(randomNumber);
		}

		public void emitBatch(long batchId, TridentCollector collector) {
			List<List<Object>> batches = new ArrayList<List<Object>>();

			for (int i = 0; i < this.batchSize; i++) {
				List<Object> tuple = this.recordGenerator();

				System.out.println(tuple);

				batches.add(tuple);
			}

			for (List<Object> list : batches) {
				collector.emit(list);
			}

			//Utils.sleep(10000);

		}

	}
	
	public static class MySum implements ReducerAggregator<Integer>{

		public Integer init() {


			// TODO Auto-generated method stub
			return 0;
		}

		public Integer reduce(Integer curr, TridentTuple tuple) {
			// TODO Auto-generated method stub
			System.out.println("---------I am in Mysum."+ Thread.currentThread().getName() );
			return curr+tuple.getInteger(0);
		}

	}

	public static void main(String[] args) {
		MyTestSpout spout = new MyTestSpout(5);
		// spout.setCycle(false);

		TridentTopology topology = new TridentTopology();

		//按照如下配置，实际运行后发现，spout有1个线程，aggregate和each共同组成一个bolt，三个线程。
		//一个批次的数据被bolt中的一个线程完整接收，既负责mysum，也负责debug。
		//但多个批次数据可以由不同的的线程负责。
		//奇怪的是在spout和aggregate之间还有一个bolt，1个线程，不知道是什么
		topology.newStream("spout1", spout)
				.partitionBy(new Fields("user"))
				.aggregate(new Fields("score", "user"), new MySum(),//Sum默认计算field(0)的结果，所以此处要反转
						new Fields("sum")).parallelismHint(3)
				.each(new Fields("sum"), new Debug("print:"));

		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Count", conf, topology.build());
		} else {
			conf.setNumWorkers(10);
			try {
				StormSubmitter.submitTopology(args[0], conf, topology.build());
			} catch (AlreadyAliveException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthorizationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}