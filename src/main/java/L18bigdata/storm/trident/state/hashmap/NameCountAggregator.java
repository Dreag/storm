package L18bigdata.storm.trident.state.hashmap;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.tuple.Values;

import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

public class NameCountAggregator implements Aggregator<Map<String, Integer>> {
	private static final long serialVersionUID = -5141558506999420908L;

	// 一个batch的一个partition调用一次。
	// 如果一个batch就1个partition，那么等效于1个batch调用一次。
	@Override
	public Map<String, Integer> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, Integer>();
	}

	// 判断某个名字是否已经存在于map中，若无，则put，若有，则递增
	@Override
	public void aggregate(Map<String, Integer> map, TridentTuple tuple,
			TridentCollector collector) {
		String key = tuple.getString(0);
		if (map.containsKey(key)) {
			Integer tmp = map.get(key);
			map.put(key, ++tmp);

		} else {
			map.put(key, 1);
		}
	}

	// 将聚合后的结果emit出去，一个batch的一个partition调用一次。
	// 如果一个batch就1个partition，那么等效于1个batch调用一次。
	@Override
	public void complete(Map<String, Integer> map, TridentCollector collector) {

		if (map.size() > 0) {

			for (Entry<String, Integer> entry : map.entrySet()) {
				System.out.println("Thread.id="
						+ Thread.currentThread().getId() + "|" + entry.getKey()
						+ "|" + entry.getValue());
				collector.emit(new Values(entry.getKey(), entry.getValue()));
			}

			map.clear();
			System.out.println("in complete");

		}
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}

}
