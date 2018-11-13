package L05bigdata.storm.trident.operation.projection;

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

class ProjectionSpout implements IBatchSpout {

	private static final long serialVersionUID = 10L;
	private int batchSize;
	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();

	private static final Map<Integer, Integer> SOURCES = new HashMap<Integer, Integer>();
	static {
		SOURCES.put(0, 1);
		SOURCES.put(1, 2);
		SOURCES.put(2, 3);
		SOURCES.put(3, 4);
		SOURCES.put(4, 5);
	}

	public ProjectionSpout(int batchSize) {
		this.batchSize = batchSize;
	}

	private List<Object> recordGenerator() {
		final Random rand = new Random();
		int randomNumber = rand.nextInt(5);
		return new Values(randomNumber, SOURCES.get(randomNumber));
	}

	@Override
	public void ack(long batchId) {
		this.batchesMap.remove(batchId);

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		List<List<Object>> batches = this.batchesMap.get(batchId);
		if (batches == null) {
			batches = new ArrayList<List<Object>>();
			for (int i = 0; i < this.batchSize; i++) {
				batches.add(this.recordGenerator());
			}
			this.batchesMap.put(batchId, batches);// 本地缓存
		}
		for (List<Object> list : batches) {
			collector.emit(list);// //尽管是一批数据，但是还是一个个发送tuple
		}

		Utils.sleep(10000);

	}

	@Override
	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("num1", "num2");
	}

	@Override
	public void open(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub

	}

}

class ProjectionPrintFunction extends BaseFunction {

	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		System.out.println(tuple);
	}

}

public class ProjectionTopology {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();

		// 在启用Ack的情况下，Spout中有个RotatingMap用来保存Spout已经发送出去，
		// 但还没有等到Ack结果的消息。RotatingMap的最大个数是有限制的，为p*num-tasks。
		// 其中p是topology.max.spout.pending值，也就是MaxSpoutPending
		// (也可以由TopologyBuilder在setSpout通过setMaxSpoutPending方法来设定)，
		// num-tasks是Spout的Task数。如果不设置MaxSpoutPending的大小或者设置得太大，
		// 可能消耗掉过多的内存导致内存溢出，设置太小则会影响Spout发射Tuple的速度。
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

		ProjectionSpout spout = new ProjectionSpout(10);
		TridentTopology topology = new TridentTopology();

		topology.newStream("spout1", spout)
				.shuffle()
				.project(new Fields("num2"))
				.each(new Fields("num2"), new ProjectionPrintFunction(),
						new Fields("")).parallelismHint(2);

		return topology.build();
	}
}
