package org.apache.shardingsphere.proxy.initializer;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.proxy.backend.config.YamlProxyConfiguration;
@Slf4j
public class ConfigParser {

    public static YamlProxyConfiguration parseConfig(String jsonString) {
        return (YamlProxyConfiguration) JSONObject.parseObject(jsonString, YamlProxyConfiguration.class);
    }


}
