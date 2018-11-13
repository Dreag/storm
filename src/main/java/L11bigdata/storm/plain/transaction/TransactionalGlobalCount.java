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
package L11bigdata.storm.plain.transaction;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.coordination.CoordinatedBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.MemoryTransactionalSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.topology.base.BaseTransactionalBolt;
import org.apache.storm.transactional.ICommitter;
import org.apache.storm.transactional.ITransactionalSpout;
import org.apache.storm.transactional.TransactionAttempt;
import org.apache.storm.transactional.TransactionalTopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a basic example of a transactional topology. It keeps a count of the number of tuples seen so far in a
 * database. The source of data and the databases are mocked out as in memory maps for demonstration purposes.
 *
 * @see <a href="http://storm.apache.org/documentation/Transactional-topologies.html">Transactional topologies</a>
 */
public class TransactionalGlobalCount {
  public static final int PARTITION_TAKE_PER_BATCH = 3;
  public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
    put(0, new ArrayList<List<Object>>() {{
      add(new Values("01s"));
      add(new Values("02s"));
      add(new Values("03s"));
      add(new Values("04s"));
      add(new Values("05s"));
      add(new Values("06s"));
      add(new Values("07s"));
      add(new Values("08s"));

    }});
    put(1, new ArrayList<List<Object>>() {{
      add(new Values("09s"));
      add(new Values("10s"));
      add(new Values("11s"));
      add(new Values("12s"));
      add(new Values("13s"));
      add(new Values("14s"));
      add(new Values("15s"));
      add(new Values("16s"));
    }});
    put(2, new ArrayList<List<Object>>() {{
      add(new Values("17s"));
      add(new Values("18s"));
      add(new Values("19s"));
      add(new Values("20s"));
      add(new Values("21s"));
      add(new Values("22s"));
      add(new Values("23s"));
      add(new Values("24s"));
    }});
  }};

  public static class Value {
    int count = 0;
    BigInteger txid;
  }

  public static Map<String, Value> DATABASE = new HashMap<String, Value>();
  public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

  
  
  
  public static class BatchCount extends BaseBatchBolt {
    Object _id;
    BatchOutputCollector _collector;

    int _count = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
      _collector = collector;
      _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
    	System.out.println("taskid为"+tuple.getSourceTask()+"的SpoutTask，"+"发送了Batchid为"+_id+"的一个tuple"+"，值为"+tuple.getString(1));
      _count++;
    }

    @Override
    public void finishBatch() {
      //System.out.println("批号"+_id+"   "+"处理Tuple的数量"+_count+"     "+Thread.currentThread().getName());
      _collector.emit(new Values(_id, _count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "count"));
    }
  }

  public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
    TransactionAttempt _attempt;
    BatchOutputCollector _collector;

    int _sum = 0;

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
      _collector = collector;
      _attempt = attempt;
    }

    @Override
    public void execute(Tuple tuple) {
      _sum += tuple.getInteger(1);
    }

    @Override
    public void finishBatch() {
      Value val = DATABASE.get(GLOBAL_COUNT_KEY);
      Value newval;
      if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
        newval = new Value();
        newval.txid = _attempt.getTransactionId();
        if (val == null) {
          newval.count = _sum;
        }
        else {
          newval.count = _sum + val.count;
        }
        DATABASE.put(GLOBAL_COUNT_KEY, newval);
      }
      else {
        newval = val;
      }
      _collector.emit(new Values(_attempt, newval.count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "sum"));
    }
  }

  public static void main(String[] args) throws Exception {
	  
	  //DATA有三块，每块称为一个partition，一个batch形成时，首要保证从每个partition中都要获取数据
	  //PARTITION_TAKE_PER_BATCH定义了对1个batch，最多从每个parrition中最多取多少个数据。
	  //所以无论有多少个spout线程，每个batch的最达Tuple数量为num(DATA_PARTITION)*PARTITION_TAKE_PER_BATCH。
	  //每一个单词只会被读取一次。
	  //当DATA中的每一个单词都被取过一次，MemoryTransactionalSpout不再发送tuple。
    MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"),3);

    //DATA有三个partition，最多使用3个spout线程来读取，
    //此时每一个线程对应一个Partition。若使用超过3个spout线程，多余的线程读取不到东西。
    //若spout线程数目小于DATA partition数目，则1个spout线程会负责多个分区。
    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout,3);
    
    
    builder.setBolt("partial-count", new BatchCount(), 5).noneGrouping("spout");
    builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial-count");

    //LocalCluster cluster = new LocalCluster();

    Config config = new Config();
    config.setDebug(true);
    config.setMaxSpoutPending(3);

    config.setNumWorkers(3);
	StormSubmitter.submitTopology(args[0], config,
			builder.buildTopology());
    
    //cluster.submitTopology("global-count-topology", config, builder.buildTopology());

//    Thread.sleep(3000);
//    cluster.shutdown();
  }
}
