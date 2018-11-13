package L08bigdata.storm.trident.streamsplit;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TridentStreamSplit {
	
	public static class ConversionFunction extends BaseFunction{

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			// TODO Auto-generated method stub
			if(tuple.getString(0).equals("Boy"))
				collector.emit(new Values(tuple.getString(1),"male"));
			if(tuple.getString(0).equals("Girl"))
				collector.emit(new Values(tuple.getString(1),"female"));
		}
		
	}
	
	public static class MaleFilter extends BaseFilter{

		@Override
		public boolean isKeep(TridentTuple tuple) {
			// TODO Auto-generated method stub
			if(tuple.getString(0).equals("male"))
				return true;
			else
				return false;
		}
		
	}
	
	public static class FemaleFilter extends BaseFilter{

		@Override
		public boolean isKeep(TridentTuple tuple) {
			// TODO Auto-generated method stub
			if(tuple.getString(0).equals("female"))
				return true;
			else
				return false;
		}
		
	}
	
	
	public static class Print extends BaseFunction{

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			// TODO Auto-generated method stub
			System.out.println(tuple.getString(0)+"  "+tuple.getString(1));
			
		}
		
	}
	
	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sex", "num"),
				                                     3,
				                                     new Values("Boy", "1"),
				                                     new Values("Boy", "2"),
				                                     new Values("Girl", "3"), 
				                                     new Values("Girl", "4")
		                                            );
		spout.setCycle(true);
		
		TridentTopology topology = new TridentTopology();
		
		Stream stream =  topology.newStream("spout", spout)
				 .shuffle()
				 .each(new Fields("sex","num"), new ConversionFunction(), new Fields("id","gender"));
		
		//Split
		Stream branch1 = stream.each(new Fields("gender"), new MaleFilter());
		Stream branch2 = stream.each(new Fields("gender"), new FemaleFilter());
		
		//Join
		topology.merge(branch1,branch2).each(new Fields("id","gender"), new Print(), new Fields("out1","out2"));
		
		
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Count", conf, topology.build());
		 
		

	}

}
