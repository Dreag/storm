package L18bigdata.storm.trident.state.hashmap;

import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.tuple.TridentTuple;

public class MyPeek implements Consumer {

	@Override
	public void accept(TridentTuple input) {
		// TODO Auto-generated method stub
		System.out.println("Peek的线程号: "+Thread.currentThread().getName());
//		System.out.println(input.getString(0)+" "+
//				           input.getInteger(1)+" "+
//				           input.getString(2)+" "+
//				           input.getString(3)
//				);
				
	}

}
