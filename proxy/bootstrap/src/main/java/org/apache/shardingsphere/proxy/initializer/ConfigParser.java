package org.apache.shardingsphere.proxy.initializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.encrypt.yaml.config.YamlEncryptRuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlGlobalRuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.apache.shardingsphere.proxy.backend.config.YamlProxyConfiguration;
import org.apache.shardingsphere.proxy.backend.config.yaml.YamlProxyServerConfiguration;

import java.util.ArrayList;
import java.util.Collection;

@Slf4j
public class ConfigParser {

    public static YamlProxyConfiguration parseConfig(String jsonString) {
        YamlProxyConfiguration yamlConfig = (YamlProxyConfiguration) JSONObject.parseObject(jsonString, YamlProxyConfiguration.class);
        JSONObject jsonObject = (JSONObject) JSON.parse(jsonString);
        // 获取 rules 数组
        Collection<String> databaseNames= jsonObject.getJSONObject("databaseConfigurations").keySet();
        for (String databaseName : databaseNames){
            JSONArray rulesArray = jsonObject.getJSONObject("databaseConfigurations").getJSONObject(databaseName).getJSONArray("rules");
            Collection<YamlEncryptRuleConfiguration> databaseConfigurations = new ArrayList<>();
            if (rulesArray != null && !rulesArray.isEmpty()) {
                for (Object rule : rulesArray) {
                    JSONObject ruleObject = (JSONObject) rule;
                    YamlEncryptRuleConfiguration encryptRuleConfiguration = JSONObject.parseObject(ruleObject.toString(), YamlEncryptRuleConfiguration.class);
                    databaseConfigurations.add(encryptRuleConfiguration);
                }
                // 处理解析后的集合
            } else {
                System.out.println("rules 数组为空或不存在");
            }
            Collection<YamlRuleConfiguration> rules = new ArrayList<>(databaseConfigurations);
            yamlConfig.getDatabaseConfigurations().get(databaseName).setRules(rules);
        }
        yamlConfig.getServerConfiguration().setRules(rebuildGlobalRuleConfiguration(yamlConfig.getServerConfiguration()).getRules());
        return  yamlConfig;
    }

    private static YamlProxyServerConfiguration rebuildGlobalRuleConfiguration(final YamlProxyServerConfiguration serverConfig) {
        serverConfig.getRules().removeIf(YamlGlobalRuleConfiguration.class::isInstance);
        if (null != serverConfig.getAuthority()) {
            serverConfig.getRules().add(serverConfig.getAuthority());
        }
        if (null != serverConfig.getTransaction()) {
            serverConfig.getRules().add(serverConfig.getTransaction());
        }
        if (null != serverConfig.getGlobalClock()) {
            serverConfig.getRules().add(serverConfig.getGlobalClock());
        }
        if (null != serverConfig.getSqlParser()) {
            serverConfig.getRules().add(serverConfig.getSqlParser());
        }
        if (null != serverConfig.getSqlTranslator()) {
            serverConfig.getRules().add(serverConfig.getSqlTranslator());
        }
        if (null != serverConfig.getLogging()) {
            serverConfig.getRules().add(serverConfig.getLogging());
        }
        if (null != serverConfig.getSqlFederation()) {
            serverConfig.getRules().add(serverConfig.getSqlFederation());
        }
        return serverConfig;
    }
}
