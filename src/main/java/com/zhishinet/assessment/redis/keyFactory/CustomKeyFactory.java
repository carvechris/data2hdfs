package com.zhishinet.assessment.redis.keyFactory;

import org.apache.storm.redis.trident.state.KeyFactory;

import java.util.List;

/**
 * <p>Title:  data2hdfs <br/> </p>
 * <p>Description TODO <br/> </p>
 * <p>Company: https://www.zhishinet.com <br/> </p>
 *
 * @Author <a herf="q315744068@gmail.com"/>Vincent Li<a/> <br/></p>
 * @Date 2018/9/17 17:05
 */
public class CustomKeyFactory implements KeyFactory {
    @Override
    public String build(List<Object> list) {
        if (list != null && list.size() > 0) {
            StringBuffer result = new StringBuffer();
            list.forEach(field -> {
                result.append(field);
                result.append("_");
            });
            return result.toString();
        } else {
            throw new RuntimeException("Default KeyFactory does not support null");
        }
    }
}
