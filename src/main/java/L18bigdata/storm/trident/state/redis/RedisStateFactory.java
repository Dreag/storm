package L18bigdata.storm.trident.state.redis;

import java.net.InetSocketAddress;
import java.util.EnumMap;
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
import org.apache.storm.trident.state.map.MapState;
import org.apache.storm.trident.state.map.NonTransactionalMap;
import org.apache.storm.trident.state.map.OpaqueMap;
import org.apache.storm.trident.state.map.SnapshottableMap;
import org.apache.storm.trident.state.map.TransactionalMap;
import org.apache.storm.tuple.Values;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public  class RedisStateFactory implements StateFactory {
    StateType type;
    InetSocketAddress server;
    Serializer serializer;
    KeyFactory factory;
    Options options;

    

    private static final EnumMap<StateType, Serializer> DEFAULT_SERIALIZERS = Maps.newEnumMap(ImmutableMap.of(
          StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer(),
          StateType.TRANSACTIONAL, new JSONTransactionalSerializer(),
          StateType.OPAQUE, new JSONOpaqueSerializer()
    ));
    
    public RedisStateFactory(InetSocketAddress server, StateType type, Options options, KeyFactory factory) {
       this.type = type;
       this.server = server;
       this.options = options;
       this.factory = factory;

       if (options.serializer == null) {
          serializer = DEFAULT_SERIALIZERS.get(type);
          if (serializer == null) {
             throw new RuntimeException("Couldn't find serializer for state type: " + type);
          }
       } else {
          this.serializer = options.serializer;
       }
    }

    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
       JedisPool pool = new JedisPool(new JedisPoolConfig(),
             server.getHostName(), server.getPort(), options.connectionTimeout, options.password, options.database);
       RedisState state = new RedisState(pool, options, serializer, factory);
       CachedMap c = new CachedMap(state, options.localCacheSize);

       MapState ms;
       if (type == StateType.NON_TRANSACTIONAL) {
          ms = NonTransactionalMap.build(c);

       } else if (type == StateType.OPAQUE) {
          ms = OpaqueMap.build(c);

       } else if (type == StateType.TRANSACTIONAL) {
          ms = TransactionalMap.build(c);

       } else {
          throw new RuntimeException("Unknown state type: " + type);
       }

       return new SnapshottableMap(ms, new Values(options.globalKey));
    }
    
    
    
    
    
    
    
    
    
    public static StateFactory opaque(InetSocketAddress server) {
        return opaque(server, new Options());
     }

     public static StateFactory opaque(InetSocketAddress server, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
        return opaque(server, opts);
     }

     public static StateFactory opaque(InetSocketAddress server, Options<OpaqueValue> opts) {
        return opaque(server, opts, new DefaultKeyFactory());
     }

     public static StateFactory opaque(InetSocketAddress server, Options<OpaqueValue> opts, KeyFactory factory) {
        return new RedisStateFactory(server, StateType.OPAQUE, opts, factory);
     }

     public static StateFactory transactional(InetSocketAddress server) {
        return transactional(server, new Options());
     }

     public static StateFactory transactional(InetSocketAddress server, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
        return transactional(server, opts);
     }

     public static StateFactory transactional(InetSocketAddress server, Options<TransactionalValue> opts) {
        return transactional(server, opts, new DefaultKeyFactory());
     }

     public static StateFactory transactional(InetSocketAddress server, Options<TransactionalValue> opts, KeyFactory factory) {
        return new RedisStateFactory(server, StateType.TRANSACTIONAL, opts, factory);
     }

     public static StateFactory nonTransactional(InetSocketAddress server) {
        return nonTransactional(server, new Options());
     }

     public static StateFactory nonTransactional(InetSocketAddress server, String hkey) {
        Options opts = new Options();
        opts.hkey = hkey;
        return nonTransactional(server, opts);
     }

     public static StateFactory nonTransactional(InetSocketAddress server, Options<Object> opts) {
        return nonTransactional(server, opts, new DefaultKeyFactory());
     }

     public static StateFactory nonTransactional(InetSocketAddress server, Options<Object> opts, KeyFactory factory) {
        return new RedisStateFactory(server, StateType.NON_TRANSACTIONAL, opts, factory);
     }

 }