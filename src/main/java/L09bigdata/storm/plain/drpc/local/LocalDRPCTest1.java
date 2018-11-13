package L09bigdata.storm.plain.drpc.local;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * @ClassName: drpcTopology
 * @Description:线性drpc学习
 * @author Administrator
 * @date 2015年10月23日
 *
 */
public class LocalDRPCTest1 {

    @SuppressWarnings("serial")
    public static class Exclation extends BaseRichBolt {
        //private static Log log = LogFactory.getLog(Exclation.class);
        private OutputCollector _outputCollector;

        @Override
        public void execute(Tuple tuple) {
            String res = null;
            try {
                res = tuple.getString(1);
                if (null != res) {
                    _outputCollector.emit(new Values(tuple.getValue(0), res + "!!!!"));//向下游发送时，必须第一个filed为drpc信息，第二个filed为参数。
                    //log.info("execute 发射消息-------->" + res);
                    System.out.println("execute 发射消息-------->" + res);
                }

            } catch (Exception e) {
                //log.error("execute处理异常！！！");
                System.out.println("execute处理异常！！！");
            }
        }

        @Override
        public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector outputCollector) {
            this._outputCollector = outputCollector;
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public static void main(String[] args) {
        // LinearDRPCTopologyBuilder帮助我们自动集成了DRPCSpout和returnResults(bolt)
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("text");
        builder.addBolt(new Exclation(), 3);

        Config config = new Config();
        LocalDRPC drpc = new LocalDRPC();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("drpc-test", config, builder.createLocalTopology(drpc));//注意需要加参数

        for (String word : new String[] { "hello", "word" }) {
            System.out.println("result \"" + word + "\": " + drpc.execute("text", word));
        }

        Utils.sleep(1000 * 5);
        drpc.shutdown();
        cluster.killTopology("drpc-test");
        cluster.shutdown();
    }

}