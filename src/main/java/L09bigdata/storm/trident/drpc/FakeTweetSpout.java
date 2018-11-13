package L09bigdata.storm.trident.drpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class FakeTweetSpout implements IBatchSpout{

	
	private static final long serialVersionUID = 10L;
	private int batchSize;
	private HashMap<Long, List<List<Object>>> batchesMap = new HashMap<Long, List<List<Object>>>();
	public FakeTweetSpout(int batchSize) {
		this.batchSize = batchSize;
	}
	
	private static final Map<Integer, String> TWEET_MAP = new HashMap<Integer, String>();
	static {
		TWEET_MAP.put(0, "#FIFA worldcup");
		TWEET_MAP.put(1, "#FIFA worldcup");
		TWEET_MAP.put(2, "#FIFA worldcup");
		TWEET_MAP.put(3, "#FIFA worldcup");
		TWEET_MAP.put(4, "#Movie top 10");
	}
	
	private static final Map<Integer, String> COUNTRY_MAP = new HashMap<Integer, String>();
	static {
		COUNTRY_MAP.put(0, "United State");
		COUNTRY_MAP.put(1, "Japan");
		COUNTRY_MAP.put(2, "India");
		COUNTRY_MAP.put(3, "China");
		COUNTRY_MAP.put(4, "Brazil");
	}
	
	private List<Object> recordGenerator() {
		final Random rand = new Random();
		int randomNumber = rand.nextInt(5);
		int randomNumber2 = rand.nextInt(5);
		System.out.println(TWEET_MAP.get(randomNumber)+"      "+COUNTRY_MAP.get(randomNumber2));

		return new Values(TWEET_MAP.get(randomNumber),COUNTRY_MAP.get(randomNumber2));
	}
	
	public void ack(long batchId) {
		this.batchesMap.remove(batchId);
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void emitBatch(long batchId, TridentCollector collector) {
//		if(batchId!=1){ //此时就发送一个batch，对比rpc的execute函数执行情况。
//			Utils.sleep(3000);
//			return;
//		}
		List<List<Object>> batches = this.batchesMap.get(batchId);
		if(batches == null) {
			batches = new ArrayList<List<Object>>();;
			for (int i=0;i < this.batchSize;i++) {
				batches.add(this.recordGenerator());
			}
			this.batchesMap.put(batchId, batches);
		}
		for(List<Object> list : batches){
            collector.emit(list);
        }
		
	}

	public Map getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public Fields getOutputFields() {
		return new Fields("text","Country");
	}

	public void open(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		
	}

}
