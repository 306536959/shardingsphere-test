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

package org.apache.shardingsphere.test.it.yaml;

import org.apache.shardingsphere.infra.util.yaml.YamlEngine;
import org.apache.shardingsphere.infra.util.yaml.datanode.RepositoryTuple;
import org.apache.shardingsphere.infra.yaml.config.pojo.YamlRootConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.YamlRuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.rule.annotation.RepositoryTupleEntity;
import org.apache.shardingsphere.mode.engine.AutoRepositoryTupleSwapperEngine;
import org.apache.shardingsphere.mode.spi.RepositoryTupleSwapper;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class RepositoryTupleSwapperIT {
    
    private final File yamlFile;
    
    @SuppressWarnings("rawtypes")
    private final RepositoryTupleSwapper swapper;
    
    private final boolean isGlobalRule;
    
    @SuppressWarnings("rawtypes")
    public RepositoryTupleSwapperIT(final String yamlFileName, final RepositoryTupleSwapper swapper, final boolean isGlobalRule) {
        URL url = Thread.currentThread().getContextClassLoader().getResource(yamlFileName);
        assertNotNull(url);
        yamlFile = new File(url.getFile());
        this.swapper = swapper;
        this.isGlobalRule = isGlobalRule;
    }
    
    @Test
    void assertSwapToRepositoryTuples() throws IOException {
        YamlRuleConfiguration yamlRuleConfig = loadYamlRuleConfiguration();
        assertRepositoryTuples(new ArrayList<>(new AutoRepositoryTupleSwapperEngine().swapToRepositoryTuples(yamlRuleConfig)), yamlRuleConfig);
    }
    
    private YamlRuleConfiguration loadYamlRuleConfiguration() throws IOException {
        YamlRootConfiguration yamlRootConfig = YamlEngine.unmarshal(yamlFile, YamlRootConfiguration.class);
        assertThat(yamlRootConfig.getRules().size(), is(1));
        return yamlRootConfig.getRules().iterator().next();
    }
    
    protected abstract void assertRepositoryTuples(List<RepositoryTuple> actualRepositoryTuples, YamlRuleConfiguration expectedYamlRuleConfig);
    
    protected void assertRepositoryTuple(final RepositoryTuple actual, final String expectedKey, final Object expectedValue) {
        assertThat(actual.getKey(), is(expectedKey));
        assertThat(actual.getValue(), is(isSimpleObject(expectedValue) ? expectedValue : YamlEngine.marshal(expectedValue)));
    }
    
    private boolean isSimpleObject(final Object expectedValue) {
        return expectedValue instanceof Boolean || expectedValue instanceof Integer || expectedValue instanceof Long || expectedValue instanceof String;
    }
    
    @Test
    void assertSwapToObject() throws IOException {
        assertThat(getActualYamlContent(), is(getExpectedYamlContent()));
    }
    
    @SuppressWarnings("unchecked")
    private String getActualYamlContent() throws IOException {
        YamlRuleConfiguration yamlRuleConfig = loadYamlRuleConfiguration();
        Collection<RepositoryTuple> repositoryTuples = new AutoRepositoryTupleSwapperEngine().swapToRepositoryTuples(yamlRuleConfig).stream()
                .map(each -> new RepositoryTuple(getRepositoryTupleKey(Objects.requireNonNull(yamlRuleConfig.getClass().getAnnotation(RepositoryTupleEntity.class)).value(), each), each.getValue()))
                .collect(Collectors.toList());
        AutoRepositoryTupleSwapperEngine repositoryTupleSwapperEngine = new AutoRepositoryTupleSwapperEngine();
        Optional<YamlRuleConfiguration> actualYamlRuleConfig = repositoryTupleSwapperEngine.swapToObject(repositoryTuples, swapper.getTypeClass());
        assertTrue(actualYamlRuleConfig.isPresent());
        YamlRootConfiguration yamlRootConfig = new YamlRootConfiguration();
        yamlRootConfig.setRules(Collections.singletonList(actualYamlRuleConfig.get()));
        return YamlEngine.marshal(yamlRootConfig);
    }
    
    private String getRepositoryTupleKey(final String ruleTypeName, final RepositoryTuple tuple) {
        return isGlobalRule
                ? String.format("/metadata/rules/%s/versions/0", ruleTypeName)
                : String.format("/metadata/foo_db/rules/%s/%s/versions/0", ruleTypeName, tuple.getKey());
    }
    
    private String getExpectedYamlContent() throws IOException {
        String content = Files.readAllLines(yamlFile.toPath()).stream()
                .filter(each -> !each.contains("#") && !each.isEmpty()).collect(Collectors.joining(System.lineSeparator())) + System.lineSeparator();
        return YamlEngine.marshal(YamlEngine.unmarshal(content, Map.class));
    }
}
