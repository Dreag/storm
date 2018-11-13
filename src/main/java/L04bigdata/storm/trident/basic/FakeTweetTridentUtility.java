package L04bigdata.storm.trident.basic;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class FakeTweetTridentUtility {
	/**
	 * Get the comma separated value as input, split the field by comma, and
	 * then emits multiple tuple as output.
	 * 
	 */
	public static class Split extends BaseFunction {

		private static final long serialVersionUID = 2L;

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String countries = tuple.getString(0);
			for (String word : countries.split(",")) {
				// System.out.println("word -"+word);
				collector.emit(new Values(word));
			}
		}
	}

	/**
	 * This class extends BaseFilter and contain isKeep method which emits only
	 * those tuple which has #FIFA in text field.
	 */
	public static class TweetFilter extends BaseFilter {

		private static final long serialVersionUID = 1L;

		public boolean isKeep(TridentTuple tuple) {
			if (tuple.getString(0).contains("#FIFA")) {
				return true;
			} else {
				return false;
			}
		}

	}

	/**
	 * This class extends BaseFilter and contain isKeep method which will print
	 * the input tuple.
	 * 
	 */
	public static class Print extends BaseFilter {

		private static final long serialVersionUID = 1L;

		public boolean isKeep(TridentTuple tuple) {
		    System.out.println(tuple);
			return true;
		}

	}

	public static class TweetCount extends BaseAggregator<TweetCount.State> {

		private static final long serialVersionUID = 1L;
		// state class


		//一个此类对象可能负责多个group的合并，并且对各组数据的处理是并发交叉运行的。
		//State是init的返回值类型，说明每组的数据都包含一个State对象。
		//因此，应该把在一个组内需要保留的信息都放到类似于State的对象内。
		 class State {                  
			long count = 0;
			String country = "";
		}

		// Initialize the state
		public State init(Object batchId, TridentCollector collector) {
			//System.out.println("I am in init."); //看看init调用几次。实测发现一个group调一次。
			return new State();
			

		}

		// Maintain the state of sum into count variable.
		public void aggregate(State state, TridentTuple tridentTuple,
				TridentCollector tridentCollector) {
			state.country = tridentTuple.getString(0);
			state.count++;
			//System.out.println(this+"    country:"+state.country+"   count:"+state.count); //看看有几个TweetCount对象，结果发现是根据并行度来的。多少并行度多少对象。
																						   //一个tweetCount对象可能负责几个group的aggregate。
			//System.out.println(state.count);
		}

		// return a tuple with two values as output
		// after processing all the tuples of given batch.
		public void complete(State state, TridentCollector tridentCollector) {
			tridentCollector.emit(new Values(state.country, state.count));
		}

	}
}
