package L18bigdata.storm.trident.state.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.TransactionalMap;

@SuppressWarnings("all")
public class JDBCState<T> implements IBackingMap<T> {

    private static JDBCStateConfig config;
    
    JDBCState(JDBCStateConfig config){
        this.config = config;
    }
    
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ").append(config.getCols())
                .append(","+config.getColVals())
                .append(",txid")
                .append(" FROM "+config.getTable())
                .append(" WHERE ")
                .append(config.getCols())
                .append("='");
        
        JDBCUtil jdbcUtil = new JDBCUtil(config.getDriver(),config.getUrl(),config.getUsername(),config.getPassword());
        
        List<Object> result = new ArrayList<Object>();
        Map<String, Object> map = null;
        for (List<Object> list : keys) {
            Object key = list.get(0);
            map = jdbcUtil.queryForMap(sqlBuilder.toString()+key+"'");
            System.out.println(sqlBuilder.toString()+key+"'"+" 【"+map);
            Bean itemBean = (Bean)map.get(key);
            long txid=0L;
            long val=0L;
            if (itemBean!=null) {
                val=itemBean.getSum();
                txid=itemBean.getTxid();
            }
            if (config.getType()==StateType.OPAQUE) {
                result.add(new OpaqueValue(txid, val));
            } else if (config.getType()==StateType.TRANSACTIONAL) {
                result.add(new TransactionalValue(txid, val));
            } else {
                result.add(val);
            }
        }
        return (List<T>) result;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {
        //构建新增SQL
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ").append(config.getTable())
                .append("("+config.getCols())
                .append(","+config.getColVals())
                .append(",txid")
                .append(",time")
                .append(") VALUES ");
        for (int i = 0; i < keys.size(); i++) {
            List<Object> key = keys.get(i);
            if (config.getType()==StateType.TRANSACTIONAL) {
                TransactionalValue val = (TransactionalValue)vals.get(i);
                sqlBuilder.append("(");
                sqlBuilder.append(key.get(0));
                sqlBuilder.append(",");
                sqlBuilder.append(val.getVal());
                sqlBuilder.append(",");
                sqlBuilder.append(val.getTxid());
                sqlBuilder.append(",NOW()");
                sqlBuilder.append("),");
            }
        }
        sqlBuilder.setLength(sqlBuilder.length()-1);
        System.out.println(sqlBuilder.toString());
        //新增数据
        JDBCUtil jdbcUtil = new JDBCUtil(config.getDriver(),config.getUrl(),config.getUsername(),config.getPassword());
        jdbcUtil.insert(sqlBuilder.toString());
    }

    public static Factory getFactory(JDBCStateConfig config) {
        return new Factory(config);
    } 
    
    static class Factory implements StateFactory {
        private static JDBCStateConfig config;
        
        public Factory(JDBCStateConfig config) {
            this.config = config;
        }
        @Override
        public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        	//此处加cacheMap包装的目的是为了可以通过其快速获取最近刚刚存入的东西，不用再查询数据库了。
            final CachedMap map = new CachedMap(new JDBCState(config), config.getCacheSize());
            System.out.println(config);
            if(config.getType()==StateType.OPAQUE) {
                return OpaqueMap.build(map);
            } else if(config.getType()==StateType.TRANSACTIONAL){
                return TransactionalMap.build(map);
            }else {
                return NonTransactionalMap.build(map);
            }
        }
    }
    
}