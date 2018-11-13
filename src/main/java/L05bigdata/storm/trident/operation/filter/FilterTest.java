package L05bigdata.storm.trident.operation.filter;


import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


public class FilterTest {
	
	public static class Debug extends BaseFilter {

		private static final long serialVersionUID = -3136720361960744881L;
		private final String name;
	    private int partitionIndex;  

	    public Debug() {
	        this(false);
	    }

	    public Debug(boolean useLogger) {
	        this.name = "DEBUG: ";
	    }

	    public Debug(String name) {
	        this.name = "DEBUG(" + name + "): ";
	    }
	    
	    @Override
	    public void prepare(Map conf, TridentOperationContext context) {
	    	this.partitionIndex = context.getPartitionIndex();  
	    	super.prepare(conf, context);
	    }

	    
	    public boolean isKeep(TridentTuple tuple) {
	           System.out.println("<"+new Date()+"[partition"+partitionIndex+"-"+Thread.currentThread().getName()+"]"+"> "+name + tuple.toString());
	        return true;
	    }
	}

	public static class MyFixedBatchSpout extends FixedBatchSpout {

		public MyFixedBatchSpout(Fields fields, int maxBatchSize, List<Object>... outputs) {
			super(fields, maxBatchSize, outputs);
		}
		
		@Override
	    public Map<String, Object> getComponentConfiguration() {
	        Config conf = new Config();
	        // 设置此组件的最大并发度
	        //conf.setMaxTaskParallelism(1);
	        return conf;
	    }
	}

    public static StormTopology buildTopology() {
    	//FixedBatchSpout, 发射出两个字段，user与score, 一个batch中包括3个tuples,7个值依次发射
        FixedBatchSpout spout = new MyFixedBatchSpout(new Fields("user", "score"), 3, 
        		new Values("john1", 4),
                new Values("john2", 7), 
                new Values("john3", 8),
                new Values("john4", 9), 
                new Values("john5", 7),
                new Values("john6", 11),
                new Values("john7", 5)
                );
        spout.setCycle(false);
        
        TridentTopology topology = new TridentTopology();
       
        topology.newStream("spout1", spout)
        		.parallelismHint(2)//设置spout的并行度为2,因为上面数据jonh1到john7一共有7条数据，则1共会发射2*7=14条数据
        		//.shuffle()
        		.partitionBy(new Fields("user"))
        		.each(new Fields("user"),new Debug("print:"))
        		.parallelismHint(5);//设置Debug并行度为5，由于使用了shuffle，14个tuple会随机分步到5个partion当中
        		
        return topology.build();
    }
    
    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(200);
        conf.setNumWorkers(30);
        conf.setMessageTimeoutSecs(100000);
        LocalCluster local = new LocalCluster();  
        local.submitTopology("test-topology", conf, buildTopology()); 
    }
}