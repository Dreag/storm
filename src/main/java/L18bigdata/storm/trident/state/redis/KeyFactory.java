package L18bigdata.storm.trident.state.redis;

import java.io.Serializable;
import java.util.List;

public  interface KeyFactory extends Serializable {
    String build(List<Object> key);
 }