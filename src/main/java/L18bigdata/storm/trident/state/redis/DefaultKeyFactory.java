package L18bigdata.storm.trident.state.redis;

import java.util.List;

public  class DefaultKeyFactory implements KeyFactory {
    public String build(List<Object> key) {
       if (key.size() != 1)
          throw new RuntimeException("Default KeyFactory does not support compound keys");
       return (String) key.get(0);
    }
 }