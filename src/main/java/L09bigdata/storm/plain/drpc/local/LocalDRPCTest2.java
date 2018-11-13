package L09bigdata.storm.plain.drpc.local;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


//由于LinearDRPCTopologyBuiler不能用了，所以这个测试使用最原始的topology构建方法。
public class LocalDRPCTest2 {
	
  public static class ExclamationBolt extends BaseBasicBolt {//drpc主要针对计算密集型运算

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("r", "s")); //向下游发送时，必须第一个filed为参数，第二个filed为drpc信息。
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String arg = tuple.getString(0);               //第一个field是参数
      Object retInfo = tuple.getValue(1);            //第二个field是drpc信息
      collector.emit(new Values(arg + "!!!", retInfo)); //向下游发送
    }

  }

  public static void main(String[] args) {
	  
	  
    TopologyBuilder builder = new TopologyBuilder();
	//LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
    
	LocalDRPC drpc = new LocalDRPC();

    DRPCSpout spout = new DRPCSpout("function", drpc);
    builder.setSpout("drpc-input", spout);
    builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc-input");
    builder.setBolt("returnbolt", new ReturnResults(), 3).shuffleGrouping("exclaim"); //最后一个bolt类型必须为ReturnResult。

    LocalCluster cluster = new LocalCluster();
    Config conf = new Config();
    conf.setDebug(false);
    cluster.submitTopology("local-drpc", conf, builder.createTopology());//注意由于手工指定了spout，此处无需加参数了。

    System.out.println("---------------"+drpc.execute("function", "aaa")+"-------------");
    System.out.println("---------------"+drpc.execute("function", "bbb")+"-------------");


  }
}
