/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.broadcast.yaml.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.broadcast.api.config.BroadcastRuleConfiguration;
import org.apache.shardingsphere.broadcast.metadata.nodepath.BroadcastRuleNodePathProvider;
import org.apache.shardingsphere.infra.config.rule.RuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.annotation.RepositoryTupleField;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.annotation.RepositoryTupleEntity;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Broadcast rule configuration for YAML.
 */
@RepositoryTupleEntity("broadcast")
@Getter
@Setter
public final class YamlBroadcastRuleConfiguration implements YamlRuleConfiguration {
    
    @RepositoryTupleField(value = BroadcastRuleNodePathProvider.TABLES, order = 0)
    private Collection<String> tables = new LinkedList<>();
    
    @Override
    public Class<? extends RuleConfiguration> getRuleConfigurationType() {
        return BroadcastRuleConfiguration.class;
    }
}
