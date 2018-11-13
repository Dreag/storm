package L06bigdata.storm.trident.aggregator.partition;

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
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import clojure.lang.Numbers;

public class PartitionAggregateTest {

	public static class MySum implements CombinerAggregator<Number> {

		@Override
		public Number init(TridentTuple tuple) { //每个tuple到来后均执行init方法，然后传给combine
			return (Number) tuple.getValue(0);
		}
		
		@Override
		public Number combine(Number val1, Number val2) {
			System.out.println("-------" + Thread.currentThread().getName()
					+ "--------MySum");
			return Numbers.add(val1, val2);
		}

		
		//如果partition中没有tuple，CombinerAggregator会发送zero()的返回值
		@Override
		public Number zero() {
			return 0;
		}


	}

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
			System.out.println("---------I am prepared."
					+ Thread.currentThread().getName());

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
				collector.emit(list);// 尽管是一批数据，但是还是一个个发送tuple
			}

			// Utils.sleep(10000);

		}

	}

	public static void main(String[] args) {
		MyTestSpout spout = new MyTestSpout(5);
		// spout.setCycle(false);

		TridentTopology topology = new TridentTopology();

		// 按照如下配置，实际运行后发现，spout有1个线程，aggregate和each共同组成一个bolt，共有3个线程。
		// 对一个批次数据，可能分成1-3个partition(输入数据决定)，每个partition由一个线程负责mysum和debug。
		topology.newStream("spout1", spout).partitionBy(new Fields("user"))
		
		
				.partitionAggregate(new Fields("score", "user"), new MySum(),// MySum默认计算field(0)的结果，所以此处要反转，参看init函数
						new Fields("sum"))
						
				
				.parallelismHint(3)		
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