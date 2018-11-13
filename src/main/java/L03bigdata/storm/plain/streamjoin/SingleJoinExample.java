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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;

/** Example of using a simple custom join bolt
 *  NOTE: Prefer to use the built-in JoinBolt wherever applicable
 */
//自己缓冲数据进行join
public class SingleJoinExample {
	
	//BaseRichBolt需要手工ack和fail
	public static class SingleJoinBolt extends BaseRichBolt {
		OutputCollector _collector;
		Fields _idFields;    //存放两个流输出tuple的fields交集，此处是id
		Fields _outFields;//本bolt拟输出的fields集合，通过构造方法传入，此处是id,age
		int _numSources;
		RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>> _pending;
		Map<String, GlobalStreamId> _fieldLocations;

		public SingleJoinBolt(Fields outFields) {
			_outFields = outFields;
		}

		public void prepare(Map conf, TopologyContext context,OutputCollector collector) {
			_fieldLocations = new HashMap<String, GlobalStreamId>();//存放本bolt输出的各field来自哪个上游哪个流
			_collector = collector;
			int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS))
					.intValue();
			
			//第一个参数List(Object)等待存储join项，此处就是“id”的值
			//第二个参数等待存储接收到的tuple及其对应的流的组合是（源X，tuple）
			_pending = new RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>>(timeout, new ExpireCallback());
			
			_numSources = context.getThisSources().size();
			
			Set<String> idFields = null;//存放上游流输出fields的交集。
			
			for (GlobalStreamId source : context.getThisSources().keySet()) {//对所有的上游流循环处理
				//获取上游某一个流的输出fields
				Fields fields = context.getComponentOutputFields(source.get_componentId(), source.get_streamId());//第一次循环获取(id,age),第二次(id,gender)
				
				
				Set<String> setFields = new HashSet<String>(fields.toList());
				if (idFields == null)
					idFields = setFields;
				else
					idFields.retainAll(setFields);//求两个上游流输出fields的交集，此处是id,存放在idFields中，交集的元素可能有多个。

				
				//确定需要输出的两个field各来自上游哪个流，将此信息存入_fieldLocations
				for (String outfield : _outFields) {
					for (String sourcefield : fields) {
						if (outfield.equals(sourcefield)) {
							_fieldLocations.put(outfield, source);//存入（gender，源1）（age，源2）
						}
					}
				}
			}
			
			
			_idFields = new Fields(new ArrayList<String>(idFields));//存入（id）

			if (_fieldLocations.size() != _outFields.size()) {
				throw new RuntimeException(
						"Cannot find all outfields among sources");
			}
		}

		public void execute(Tuple tuple) {
			List<Object> id = tuple.select(_idFields);//获取当前tuple的id项,因为_idFields在prepare方法中已确定
			GlobalStreamId streamId = new GlobalStreamId(
					tuple.getSourceComponent(), tuple.getSourceStreamId());//从当前tuple获取其所属流
			
			if (!_pending.containsKey(id)) {
				_pending.put(id, new HashMap<GlobalStreamId, Tuple>());
			}
			
			
			Map<GlobalStreamId, Tuple> parts = _pending.get(id);//此时parts里不一定有数据
			if (parts.containsKey(streamId))
				throw new RuntimeException(
						"Received same side of single join twice");
			parts.put(streamId, tuple);
			
			
			if (parts.size() == _numSources) {//两个需配对儿的tuple均到达
				_pending.remove(id);
				List<Object> joinResult = new ArrayList<Object>();
				for (String outField : _outFields) {
					GlobalStreamId loc = _fieldLocations.get(outField);
					joinResult.add(parts.get(loc).getValueByField(outField));
				}
				_collector.emit(new ArrayList<Tuple>(parts.values()), joinResult);//一个输出tuple是需要和两个输入tuple锚定的。

				for (Tuple part : parts.values()) {
					_collector.ack(part);//告知上游本次处理的两个tuple都成功。
				}
			}
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(_outFields);
		}

		private class ExpireCallback implements RotatingMap.ExpiredCallback<List<Object>, Map<GlobalStreamId, Tuple>> {
			public void expire(List<Object> id, Map<GlobalStreamId, Tuple> tuples) {//对应某个id的tuple，只要有一个超时，全部fail，要求上游重发。
				for (Tuple tuple : tuples.values()) {
					_collector.fail(tuple);
				}
			}
		}
	}
	
	
  public static void main(String[] args) {
    FeederSpout genderSpout = new FeederSpout(new Fields("id", "gender"));
    FeederSpout ageSpout = new FeederSpout(new Fields("id", "age"));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("gender", genderSpout);
    builder.setSpout("age", ageSpout);
    
    //一个bolt接收两个上游流
    builder.setBolt("join", new SingleJoinBolt(new Fields("gender", "age"))).fieldsGrouping("gender", new Fields("id"))
        .fieldsGrouping("age", new Fields("id"));//来自上游的相同id的tuple到下游同一个组件中处理。

    Config conf = new Config();
    conf.setDebug(false);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("join-example", conf, builder.createTopology());

    //数据模拟本想放在feederspout后面执行，发现不行，必须放在main函数靠后位置。
    //因为只有当topology启动后，才开始喂数据。
    for (int i = 0; i < 10; i++) {
      String gender;
      if (i % 2 == 0) {
        gender = "male";
      }
      else {
        gender = "female";
      }
      genderSpout.feed(new Values(i, gender));
    }

    for (int i = 9; i >= 0; i--) {
      ageSpout.feed(new Values(i, i + 20));
    }

    Utils.sleep(2000);
    cluster.shutdown();
  }
}
