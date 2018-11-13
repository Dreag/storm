package bigdata.storm.trident.itridentspout;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.JSONNonTransactionalSerializer;
import org.apache.storm.trident.state.JSONOpaqueSerializer;
import org.apache.storm.trident.state.JSONTransactionalSerializer;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.TransactionalValue;
import org.apache.storm.trident.state.map.CachedMap;
import org.apache.storm.trident.state.map.IBackingMap;
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import L18bigdata.storm.trident.state.redis.KeyFactory;
import L18bigdata.storm.trident.state.redis.Options;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class MyRedisState<T> implements IBackingMap<T> {
	private static final Logger logger = LoggerFactory
			.getLogger(MyRedisState.class);

	private final JedisPool pool;
	private Options options;
	private Serializer serializer;
	private KeyFactory keyFactory;

	public MyRedisState(JedisPool pool, Options options,
			Serializer<T> serializer, KeyFactory keyFactory) {
		this.pool = pool;
		this.options = options;
		this.serializer = serializer;
		this.keyFactory = keyFactory;
	}

	public List<T> multiGet(List<List<Object>> keys) {
		if (keys.size() == 0) {
			return Collections.emptyList();
		}
		if (Strings.isNullOrEmpty(this.options.hkey)) {
			String[] stringKeys = buildKeys(keys);
			List<String> values = mget(stringKeys);
			return deserializeValues(keys, values);
		} else {
			Map<byte[], byte[]> keyValue = hgetAll(this.options.hkey.getBytes());
			List<String> values = buildValuesFromMap(keys, keyValue);
			return deserializeValues(keys, values);
		}
	}

	private List<String> buildValuesFromMap(List<List<Object>> keys,
			Map<byte[], byte[]> keyValue) {
		List<String> values = new ArrayList<String>(keys.size());
		for (List<Object> key : keys) {
			String strKey = keyFactory.build(key);
			byte[] bytes = keyValue.get(strKey.getBytes());
			values.add(bytes == null ? null : new String(bytes));
		}
		return values;
	}

	private List<T> deserializeValues(List<List<Object>> keys,
			List<String> values) {
		List<T> result = new ArrayList<T>(keys.size());
		for (String value : values) {
			if (value != null) {
				result.add((T) serializer.deserialize(value.getBytes()));
			} else {
				result.add(null);
			}
		}
		return result;
	}

	private String[] buildKeys(List<List<Object>> keys) {
		String[] stringKeys = new String[keys.size()];
		int index = 0;
		for (List<Object> key : keys)
			stringKeys[index++] = keyFactory.build(key);
		return stringKeys;
	}

	private List<String> mget(String... keys) {
		Jedis jedis = pool.getResource();
		try {
			return jedis.mget(keys);
		} finally {
			pool.returnResource(jedis);
		}
	}

	private Map<byte[], byte[]> hgetAll(byte[] bytes) {
		Jedis jedis = pool.getResource();
		try {
			return jedis.hgetAll(bytes);
		} finally {
			pool.returnResource(jedis);
		}
	}

	public void multiPut(List<List<Object>> keys, List<T> vals) {
		
		System.out.println("in multiput.");

		if (keys.size() == 0) {
			return;
		}

		if (Strings.isNullOrEmpty(this.options.hkey)) {
			String[] keyValues = buildKeyValuesList(keys, vals);
			mset(keyValues);
		} else {
			Jedis jedis = pool.getResource();
			try {
				Pipeline pl = jedis.pipelined();
				pl.multi();

				for (int i = 0; i < keys.size(); i++) {

					String val = new String(serializer.serialize(vals.get(i)));
					pl.hset(this.options.hkey, keyFactory.build(keys.get(i)),
							val);
				}

				pl.exec();
				pl.sync();
			} finally {
				pool.returnResource(jedis);
			}
		}
	}

	private String[] buildKeyValuesList(List<List<Object>> keys, List<T> vals) {
		String[] keyValues = new String[keys.size() * 2];
		for (int i = 0; i < keys.size(); i++) {

			keyValues[i * 2] = keyFactory.build(keys.get(i));
			keyValues[i * 2 + 1] = new String(serializer.serialize(vals.get(i)));
		}
		return keyValues;
	}

	private void mset(String... keyValues) {
		Jedis jedis = pool.getResource();
		try {
			jedis.mset(keyValues);
		} finally {
			pool.returnResource(jedis);
		}
	}
}
