package L18bigdata.storm.trident.state.hashmap;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/*
 * 本拓扑统计了每个名字出现的次数。
 * 主要用于演示了Trident State的用法，以及partitionAggregate的用法。
 */
public class NameSumTopology {

	private StormTopology buildTopology() {
		
		
		MyFixedBatchSpout spout = new MyFixedBatchSpout(
				new Fields("name","age","title","tel"),
				5,
				new Values("jason",20,"op","15810005758"),
				new Values("john",20,"rd","13254568412"),
				new Values("joe",21,"rd","18756427333"),
				new Values("nancy",22,"hr","13698756254"),
				new Values("lucy",23,"op","132547524787"),
				new Values("ken",24,"op","1552368742554"),
				new Values("hawk",20,"ceo","159456852356"),
				new Values("rocky",22,"cto","1325687456"),
				new Values("chris",23,"cfo","13756425896"),
				new Values("jason",20,"coo","13565478952"),
				new Values("leo",21,"cxo","13452687922")
			);

		spout.setCycle(false); //不是指只发送1个batch，而是指最多发送   数据项总数/每批次最大数据项个数，上面的例子里，即发送11/maxBatchSize个批次

		TridentTopology topology = new TridentTopology();

		Stream stream1 = topology
				.newStream("tridentStateDemoId", spout)
				.parallelismHint(3) //为此spout设置并行度总是不起效，不知为何。
				.shuffle()           //此处如果没有repartition操作，后面的parallelismHint不起作用，peek是单线程，加上后，peek是多线程。
				.peek(new MyPeek())
				.parallelismHint(4)
				.partitionBy(new Fields("name"));   //根据name的值作分区。由于下游并行度的关系，这里实际分的partition数量为1。
		
		Stream stream2 = stream1.partitionAggregate(new Fields("name"), new NameCountAggregator(),
				new Fields("nameSumKey", "nameSumValue"));
				
		TridentState state = stream2.partitionPersist(  //分区聚合不提供自动的事务处理（？好像如此），而persistAggregate提供。
				new NameSumStateFactory(), new Fields("nameSumKey", "nameSumValue"),
				new NameSumUpdater());

		return topology.build();

	}

	public static void main(String[] args) {

		NameSumTopology myTopology = new NameSumTopology();
		Config config = new Config();
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("kafka-storm", config,
				myTopology.buildTopology());

	}

}
