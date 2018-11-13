package L18bigdata.storm.trident.state.hashmap;

import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;




public class NameSumStateFactory implements StateFactory {
	
	private static final long serialVersionUID = 8753337648320982637L;

	@Override
	public State makeState(Map arg0, IMetricsContext arg1, int arg2, int arg3) {
		return new NameSumState();  
	} 
}