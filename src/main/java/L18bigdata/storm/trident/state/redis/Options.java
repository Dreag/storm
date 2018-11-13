package L18bigdata.storm.trident.state.redis;

import java.io.Serializable;

import org.apache.storm.trident.state.Serializer;

import redis.clients.jedis.Protocol;



public  class Options<T> implements Serializable {
   public int localCacheSize = 1000;
   public String globalKey = "$REDIS-MAP-STATE-GLOBAL";
   public Serializer<T> serializer = null;
   public KeyFactory keyFactory = null;
   public int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
   public String password = null;
   public int database = Protocol.DEFAULT_DATABASE;
   public String hkey = null;
}