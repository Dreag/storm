package L09bigdata.storm.plain.drpc.remote;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;




public class RemoteDRPCClient {
	
	//这是DRPC的远程客户端，需要DRPC正式提交到集群上运行后才能使用。
	public static void main(String[] args) throws Exception {
		
		//注意下面一句话一定要这样写，否则客户端运行报错。
		//很多参考资料都是写 Config = new Config();
		Map cfg = Utils.readDefaultConfig();
		
		DRPCClient client = new DRPCClient(cfg,"192.168.120.201",3772);
		

		for (String word : new String[] { "hello", "goodbye" }) {
			System.out.println("Result for \"" + word + "\": "
					+ client.execute("function", word));
		}


	}
}
