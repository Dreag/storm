package L18bigdata.storm.trident.state.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.tuple.Values;

import com.google.common.collect.Maps;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class MyHbaseState<T> implements IBackingMap<T> {

    private static final Map<StateType, Serializer> DEFAULT_SERIALZERS = Maps
            .newHashMap();

    private int partitionNum;

    private Options<T> options;

    private Serializer<T> serializer;

    private Connection connection;

    private Table table;

    static {
        DEFAULT_SERIALZERS.put(StateType.NON_TRANSACTIONAL,
                new JSONNonTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.TRANSACTIONAL,
                new JSONTransactionalSerializer());
        DEFAULT_SERIALZERS.put(StateType.OPAQUE, new JSONOpaqueSerializer());
    }

    public MyHbaseState(final Options<T> options, Map conf, int partitionNum) {
        this.options = options;
        this.serializer = options.serializer;
        this.partitionNum = partitionNum;
        try {
            connection = ConnectionFactory.createConnection(HBaseConfiguration
                    .create());
            table = connection.getTable(TableName.valueOf(options.tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Options<T> implements Serializable {

        /**
         *
         */
        private static final long serialVersionUID = 1L;

        public Serializer<T> serializer = null;

        public String globalkey = "$HBASE_STATE_GLOBAL$";

        /**
         * 表名
         */
        public String tableName;

        /**
         * 列族
         */
        public String columFamily;

        /**
         *
         */
        public String qualifier;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getColumFamily() {
            return columFamily;
        }

        public void setColumFamily(String columFamily) {
            this.columFamily = columFamily;
        }

        public String getQualifier() {
            return qualifier;
        }

        public void setQualifier(String qualifier) {
            this.qualifier = qualifier;
        }

    }

    protected static class HbaseFactory<T> implements StateFactory {

        private static final long serialVersionUID = 1L;
        private Options<T> options;

        public HbaseFactory(Options<T> options) {
            this.options = options;
            if (this.options.serializer == null) {
                this.options.serializer = DEFAULT_SERIALZERS
                        .get(StateType.OPAQUE);
            }
        }

        @Override
        public State makeState(Map conf, IMetricsContext metrics,
                int partitionIndex, int numPartitions) {
            System.out.println("partitionIndex:" + partitionIndex
                    + ",numPartitions:" + numPartitions);
            IBackingMap state = new MyHbaseState(options, conf, partitionIndex);
            MapState mapState = OpaqueMap.build(state);
            return new SnapshottableMap(mapState, new Values(options.globalkey));
        }

    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        List<Put> puts = new ArrayList<Put>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            Put put = new Put(toRowKey(keys.get(i)));
            T val = values.get(i);
            System.out.println("partitionIndex: " + this.partitionNum
                    + ",key.get(i):" + keys.get(i) + "value值:" + val);
            put.addColumn(this.options.columFamily.getBytes(),
                    this.options.qualifier.getBytes(),
                    this.options.serializer.serialize(val));
            puts.add(put);
        }
        try {
            this.table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        List<Get> gets = new ArrayList<Get>();
        for (final List<Object> key : keys) {
            // LOG.info("Partition: {}, GET: {}", this.partitionNum, key);
            Get get = new Get(toRowKey(key));
            get.addColumn(this.options.columFamily.getBytes(),
                    this.options.qualifier.getBytes());
            gets.add(get);
        }
        List<T> retval = new ArrayList<T>();
        try {
            // 批量获取所有rowKey的数据
            Result[] results = this.table.get(gets);
            for (final Result result : results) {
                byte[] value = result.getValue(
                        this.options.columFamily.getBytes(),
                        this.options.qualifier.getBytes());
                if (value != null) {
                    retval.add(this.serializer.deserialize(value));
                } else {
                    retval.add(null);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return retval;
    }

    private byte[] toRowKey(List<Object> keys) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            for (Object key : keys) {
                bos.write(String.valueOf(key).getBytes());
            }
            bos.close();
        } catch (IOException e) {
            throw new RuntimeException("IOException creating HBase row key.", e);
        }
        return bos.toByteArray();
    }

}