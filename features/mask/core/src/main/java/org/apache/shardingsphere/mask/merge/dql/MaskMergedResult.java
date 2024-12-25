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

package org.apache.shardingsphere.mask.merge.dql;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.binder.context.segment.select.projection.impl.ColumnProjection;
import org.apache.shardingsphere.infra.binder.context.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.mask.rule.MaskRule;
import org.apache.shardingsphere.mask.rule.MaskTable;
import org.apache.shardingsphere.mask.spi.MaskAlgorithm;

import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Optional;

/**
 * Merged result for mask.
 */
@RequiredArgsConstructor
public final class MaskMergedResult implements MergedResult {
    
    private final MaskRule maskRule;
    
    private final SelectStatementContext selectStatementContext;
    
    private final MergedResult mergedResult;
    
    @Override
    public boolean next() throws SQLException {
        return mergedResult.next();
    }

    /**
     * 根据列索引和类型获取值。
     * 首先尝试查找是否存在对应的列投影，如果不存在或对应的表没有掩码规则，或找不到具体的掩码算法，则直接返回合并结果的原始值。
     * 如果存在对应的列投影、掩码表和掩码算法，则获取原始值并应用掩码算法后返回。
     *
     * @param columnIndex 列索引。
     * @param type 期望的值类型。
     * @return 应用掩码算法后的值或原始值。
     * @throws SQLException 如果获取值失败。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Object getValue(final int columnIndex, final Class<?> type) throws SQLException {
        // 尝试查找列投影
        Optional<ColumnProjection> columnProjection = selectStatementContext.getProjectionsContext().findColumnProjection(columnIndex);
        if (!columnProjection.isPresent()) {
            return mergedResult.getValue(columnIndex, type);
        }
        String value = columnProjection.get().getOriginalTable().getValue();
        System.out.println(value);
        // 查找掩码表
        Optional<MaskTable> maskTable = maskRule.findMaskTable(columnProjection.get().getOriginalTable().getValue());
        if (!maskTable.isPresent()) {
            return mergedResult.getValue(columnIndex, type);
        }
        String value1 = columnProjection.get().getName().getValue();
        System.out.println(value1);
        // 查找掩码算法
        Optional<MaskAlgorithm> maskAlgorithm = maskTable.get().findAlgorithm(columnProjection.get().getName().getValue());
        if (!maskAlgorithm.isPresent()) {
            return mergedResult.getValue(columnIndex, type);
        }
        // 获取原始值并应用掩码算法
        Object originalValue = mergedResult.getValue(columnIndex, Object.class);
        return null == originalValue ? null : maskAlgorithm.get().mask(originalValue);
    }


    @Override
    public Object getCalendarValue(final int columnIndex, final Class<?> type, final Calendar calendar) throws SQLException {
        return mergedResult.getCalendarValue(columnIndex, type, calendar);
    }
    
    @Override
    public InputStream getInputStream(final int columnIndex, final String type) throws SQLException {
        return mergedResult.getInputStream(columnIndex, type);
    }
    
    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        return mergedResult.getCharacterStream(columnIndex);
    }
    
    @Override
    public boolean wasNull() throws SQLException {
        return mergedResult.wasNull();
    }
}
