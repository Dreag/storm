package L07bigdata.storm.trident.groupby;

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

//groupby 后跟agrregate
public class GroupByTest2 {

	public static class MyDebugBaseFilter extends BaseFilter {

		private static final long serialVersionUID = -3136720361960744881L;
		private final String name;
		private int partitionIndex;

		public MyDebugBaseFilter() {
			this(false);
		}

		public MyDebugBaseFilter(boolean useLogger) {
			this.name = "DEBUG: ";
		}

		public MyDebugBaseFilter(String name) {
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

			SOURCES.add(new Values("name4", 2));
			SOURCES.add(new Values("name5", 3));
			SOURCES.add(new Values("name4", 1));
			SOURCES.add(new Values("name5", 4));

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

	public static class MySumCombinerAggregator implements
			CombinerAggregator<Integer> {

		// partition当中的每个tuple调用 1次
		public Integer init(TridentTuple tuple) {
			return tuple.getInteger(0);
		}

		// 聚合结果
		// 第1次调用时，val1值为zero返回的值,之后的调用为上次调用 combine的返回值
		// val2为每次init返回的值ֵ
		public Integer combine(Integer val1, Integer val2) {
			return val1 + val2;
		}

		// 如果partition如此没有tuple，也会调用
		public Integer zero() {
			return 0;
		}

	}

	public static void main(String[] args) {
		MyTestSpout spout = new MyTestSpout(5);
		// spout.setCycle(false);

		TridentTopology topology = new TridentTopology();

		topology.newStream("spout1", spout)
				.shuffle()
				.each(new Fields("user", "score"),
						new MyDebugBaseFilter("shuffle print:"))
				.parallelismHint(5)
				.groupBy(new Fields("user"))
				.aggregate(new Fields("score"), new MySumCombinerAggregator(),
						new Fields("sum"))
				.each(new Fields("user", "sum"),
						new MyDebugBaseFilter("sum print:")).parallelismHint(5);

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
