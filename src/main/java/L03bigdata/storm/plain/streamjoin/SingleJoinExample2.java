/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package L03bigdata.storm.plain.streamjoin;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

/**
 * Example of using a simple custom join bolt NOTE: Prefer to use the built-in
 * JoinBolt wherever applicable
 */
//更为正式的join
public class SingleJoinExample2 {

	private static class PrintBolt extends BaseBasicBolt {

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			// TODO Auto-generated method stub
			System.out.println(input.getInteger(0) + " " + input.getString(1)
					+ " " + input.getInteger(2));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) {
		FeederSpout genderSpout = new FeederSpout(new Fields("id1", "gender"));
		FeederSpout ageSpout = new FeederSpout(new Fields("id2", "age"));

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("gender_spout", genderSpout);
		builder.setSpout("age_spout", ageSpout);

		JoinBolt joinBolt = new JoinBolt("gender_spout", "id1")
				.join("age_spout", "id2", "gender_spout")  //inner join.还支持leftJoin
				.select("id1,gender,age")
				.withTumblingWindow(Duration.seconds(20)); //每隔20秒匹配一次，匹配到了向下游输出，匹配不到数据过期删除。

		// 一个bolt接收两个上游流
		builder.setBolt("join_bolt", joinBolt)
				.fieldsGrouping("gender_spout", new Fields("id1"))
				.fieldsGrouping("age_spout", new Fields("id2"));

		PrintBolt printBolt = new PrintBolt();

		builder.setBolt("print_bolt", printBolt).shuffleGrouping("join_bolt");

		Config conf = new Config();
		conf.setDebug(false);

		conf.setMessageTimeoutSecs(300);// 消息过期时间设置的足够大,否则报错。

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("join-example2", conf, builder.createTopology());

		// 数据模拟本想放在feederspout后面执行，发现不行，必须放在main函数靠后位置。
		// 因为只有当topology启动后，才开始喂数据。

		while (true){
			for (int i = 0; i < 100; i++) {
				
				Utils.sleep(1000); //1秒输出一个配对

				String gender;
				if (i % 2 == 0) {
					gender = "male";
				} else {
					gender = "female";
				}

				ageSpout.feed(new Values(i, i+1));
				genderSpout.feed(new Values(i, gender)); //两个id不能相隔太远，要考虑窗口的数据过期
			}

		// for (int i = 9; i >= 0; i--) {
		// ageSpout.feed(new Values(i, i + 20));
		// }
		}
		
		//cluster.shutdown();
	}
}
