package L05bigdata.storm.trident.operation.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;




 class SumSpout implements IBatchSpout {

	private static final long serialVersionUID = 10L;
	private int batchSize;
	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();

	public SumSpout(int batchSize) {
		this.batchSize = batchSize;
	}

	private static final Map<Integer, Integer> SOURCES = new HashMap<Integer, Integer>();
	static {
		SOURCES.put(0,1);
		SOURCES.put(1,2);
		SOURCES.put(2,3);
		SOURCES.put(3,4);
		SOURCES.put(4,5);
	}


	private List<Object> recordGenerator() {
		final Random rand = new Random();
		int randomNumber = rand.nextInt(5);
		return new Values(randomNumber,SOURCES.get(randomNumber));
	}

	public void ack(long batchId) {
		this.batchesMap.remove(batchId);

	}

	public void close() {
		// TODO Auto-generated method stub

	}

	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batches = this.batchesMap.get(batchId);
		if (batches == null) {
			batches = new ArrayList<List<Object>>();
			for (int i = 0; i < this.batchSize; i++) {
				batches.add(this.recordGenerator());
			}
			this.batchesMap.put(batchId, batches);//本地缓存
		}
		for (List<Object> list : batches) {
			collector.emit(list);//尽管是一批数据，但是还是一个个发送tuple
		}
		System.out.println("--------SPOUT---------"+Thread.currentThread().getName());
		Utils.sleep(5000);

	}

	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("num1", "num2");
	}

	public void open(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub

	}

}
 
  class SumPrintFunction extends BaseFunction {

		public void execute(TridentTuple tuple, TridentCollector collector) {
			// TODO Auto-generated method stub
			System.out.println("--------PRINT---------"+Thread.currentThread().getName()+"  "+this);
			System.out.println(tuple);
		}


	}
  
   class SumFunction extends BaseFunction {
		private static final long serialVersionUID = 5L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			int number1 = tuple.getInteger(0);
			int number2 = tuple.getInteger(1);
			int sum = number1 + number2;
			System.out.println("--------SUM---------"+Thread.currentThread().getName()+"  "+this);

			// emit the sum of first two fields
			collector.emit(new Values(sum));
		}
	}

public class SumFunctionTopology {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Count", conf, buildTopology());
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}

	public static StormTopology buildTopology() {

		SumSpout spout = new SumSpout(5);
		TridentTopology topology = new TridentTopology();

		topology.newStream("spout1", spout).parallelismHint(3)
				.shuffle()
				.each(new Fields("num1", "num2"), new SumFunction(),
						new Fields("sum"))
				.shuffle()
				.each(new Fields("num1", "num2", "sum"),
						new SumPrintFunction(), new Fields(""))
				.parallelismHint(5);

		return topology.build();
	}
}
