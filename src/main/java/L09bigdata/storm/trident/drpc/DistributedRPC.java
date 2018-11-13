package L09bigdata.storm.trident.drpc;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class DistributedRPC {
		
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		
		//在启用Ack 的情况下，Spout 中有个RotatingMap 用来保存Spout 已经发送出去，
		//但还没有等到Ack 结果的消息。RotatingMap 的最大个数是有限制的，为p*num-tasks。
		//其中p 是topology.max.spout.pending 值，也就是MaxSpoutPending。
		//也可以由TopologyBuilder在setSpout 通过setMaxSpoutPending 方法来设定。
		//num-tasks 是Spout 的Task 数。如果不设置MaxSpoutPending的大小或者设置得太大，
		//可能消耗掉过多的内存导致内存溢出，设置太小则会影响Spout发射Tuple的速度。
		conf.setMaxSpoutPending(1000);
		
		if (args.length == 0) {//模拟运行
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("CountryCount", conf, buildTopology(drpc));
			
			Thread.sleep(2000);//等待topology启动，然后再开始查。
			
			for(int i=0; i<100 ; i++) {
				System.out.println("Result - "+drpc.execute("queryFunction", "Japan")); //第一个参数是函数名
				Thread.sleep(3000);
				}
		} 
		
		else {//集群运行
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null));

		}
	}

	
	public static StormTopology buildTopology(LocalDRPC drpc) {

		FakeTweetSpout spout = new FakeTweetSpout(20);
		TridentTopology topology = new TridentTopology();
		
		//一个topology创建两个stream，一个是数据处理stream，一个是drpc stream。
		
		
		//创建一个名为spout1的流，从spout中接收数据
		TridentState countryCount = topology.newStream("spout1", spout) //返回值countryCount是由persistentAggregate产生的，当然可以查询。
				.shuffle()
				.each(new Fields("text","Country"), new TridentUtility.TweetFilter())
				.groupBy(new Fields("Country"))
				.persistentAggregate(new MemoryMapState.Factory(),new Fields("Country"), new Count(), new Fields("count"))//在内存中形成（Country-count）对
				.parallelismHint(2);
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		
		topology.newDRPCStream("queryFunction", drpc)//第一个参数是定义的函数名
		.each(new Fields("args"), new TridentUtility.Split(), new Fields("Country_to_be_queried"))	
		
		//由于内存中形成（key，value）结构，
		//所以下面的方法中第一个参数是待查询对象
		//第二个是待查询key的值
		//第三个是查询得到的value的值赋予的field值。
		//注意，经过测试发现，最后一个参数中field的值随便写，没有必要一定是上个流中的“count”。
		.stateQuery(countryCount, new Fields("Country_to_be_queried"), new MapGet(),new Fields("count"));
		//.each(new Fields("count"),new FilterNull());
		//过滤掉查询出来的结果为null的情况。
		//如果给此行注释掉，前面drpc.execute的某“一”次调用返回结果是
		//[["Japan,India,Europe,China","Japan",null],["Japan,India,Europe,China","India",3],["Japan,India,Europe,China","Europe",null],["Japan,India,Europe,China","China",1]]
		return topology.build();
	}
}
