package L09bigdata.storm.trident.drpc;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class ClientRemote {
	public static void main(String[] args) throws Exception {

		//Config cfg = new Config();//这样会报错。
		Map cfg = Utils.readDefaultConfig(); 
		DRPCClient client = new DRPCClient(cfg, "node01", 3772);
		System.out.print(client.execute("queryFunction", "China"));

	}

}
