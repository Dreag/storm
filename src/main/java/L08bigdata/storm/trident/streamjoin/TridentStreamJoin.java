package L08bigdata.storm.trident.streamjoin;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.config.TumblingDurationWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

//流的合并操作，是指根据两个流的关联条件将两个流合并成一个流，然后在进行后面的处理操作
//如果使用Spout和Bolt这种编程模型的话写起来会比较困难和繁琐，因为要设置缓冲区来保存第一次过来的数据，
//后续还要进行两者的比较，使用Trident应用起来比较方便，对原来的编程模型进行了一定的抽象。代码实例：
//
//需求：
//
//        两个spout： spout1:里面的数据是 name ,id 和tel,
//
//        spout2是sex 和id；
//
//        首先对spout1进行过滤，过滤掉不是186的电话号码，然后显示
//
//        然后根据将过滤后的stream和spout2进行合并

public class TridentStreamJoin {

	public static class Filter186 extends BaseFilter {

		public boolean isKeep(TridentTuple tuple) {
			String get = tuple.getString(2);
			if (get.startsWith("186")) {
				return true;
			}
			return false;
		}
	}

	public static class NothingFunction extends BaseFunction {

		public void execute(TridentTuple tuple, TridentCollector collector) {
			String getName = tuple.getString(0);
			String getid = tuple.getString(1);
			String getTel = tuple.getString(2);
			// System.out.println("过滤后数据：============" + tuple.getValues());
			collector.emit(new Values(getName, getid, getTel));
		}

	}

	public static class PrintFunction extends BaseFunction {

		public void execute(TridentTuple tuple, TridentCollector collector) {

			System.out.println(tuple.getValues());
		}

	}

	public static void main(String[] args) {

		FixedBatchSpout spout1 = new FixedBatchSpout(new Fields("name",
				"idName", "tel"), 3, new Values("Jack", "1", "186107"),
				new Values("Tome", "2", "1514697"), new Values("Lay", "3",
						"186745"), new Values("Lucy", "4", "1396478"));

		FixedBatchSpout spout2 = new FixedBatchSpout(
				new Fields("sex", "idSex"), 3, new Values("Boy", "1"),
				new Values("Boy", "2"), new Values("Girl", "3"), new Values(
						"Girl", "4"));
		// 设置是否循环
		 spout1.setCycle(true);
		 spout2.setCycle(true);

		// 构建Trident的Topo
		TridentTopology topology = new TridentTopology();
		
		// 定义过滤器： 保留186开头电话的Tuple
		Filter186 filter186 = new Filter186();
		// 定义方法 用来显示过滤后的数据
		NothingFunction nothingFunction = new NothingFunction();

		PrintFunction printFunction = new PrintFunction();

		// 根据spout构建第一个Stream
		Stream st1 = topology.newStream("sp1", spout1);
		// 对第一个Stream数据显示。
		// Stream st1_1 = st1.each(new Fields("name", "idName", "tel"),
		// nothingFunction,
		// new Fields("out_name", "out_idName", "out_tel"));
		// 根据第二个Spout构建Stream，为了测试join用
		Stream st2 = topology.newStream("sp2", spout2);
		/**
		 * 开始Join st和st2这两个流，类比sql中join的话是： st join st2 on joinFields1 =
		 * joinFields2 需要注意的是以st1为数据基础 topology.join(st1, new Fields("idName"),
		 * st2, new Fields("idSex"), new Fields("id","name","tel","sex"))
		 * 那么结果将是以spout为数据基础，结果会将上面的4个数据信息全部打出
		 */
		Stream st_join = topology.join(st1, new Fields("idName"),   //以batch为单位聚合，从此角度来说，这个程序的意义不大。
			                           st2,new Fields("idSex"),
				// 输出字段连接字段、第一个流的从左自右剩余字段、
				// 第二个流的从左至右剩余字段]
				new Fields("Res_id", "Res_name", "Res_tel", "Res_sex"));
		// 创建一个方法，为了显示合并和过滤后的结果
		st_join.each(new Fields("Res_id", "Res_name", "Res_tel", "Res_sex"),
				filter186).each(
				new Fields("Res_id", "Res_name", "Res_tel", "Res_sex"),
				printFunction, new Fields());

		Config cf = new Config();
		cf.setNumWorkers(2);
		//cf.setNumAckers(0); //不能设为0，设为0运行抛出StackOverflowError异常
		cf.setMaxSpoutPending(2);//spout已发出的，尚未得到确认的最大batch个数，超出该个数spout会暂停发送。
		cf.setDebug(false);
		//cf.setMessageTimeoutSecs(1000);

		LocalCluster lc = new LocalCluster();
		lc.submitTopology("TestTopo", cf, topology.build());
	}
}