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

package org.apache.shardingsphere.infra.rewrite.sql.impl;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.rewrite.sql.SQLBuilder;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Attachable;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.Substitutable;
import org.apache.shardingsphere.infra.rewrite.sql.token.common.pojo.generic.ComposableSQLToken;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Abstract SQL builder.
 */
@RequiredArgsConstructor
public abstract class AbstractSQLBuilder implements SQLBuilder {
    
    private final String sql;
    
    private final List<SQLToken> sqlTokens;
    
    @Override
    public final String toSQL() {
        if (sqlTokens.isEmpty()) {
            return sql;
        }
        Collections.sort(sqlTokens);
        StringBuilder result = new StringBuilder(sql.length());
        result.append(sql, 0, sqlTokens.get(0).getStartIndex());
        Optional<SQLToken> previousToken = Optional.empty();
        for (SQLToken each : sqlTokens) {
            if (isContainsAttachableToken(each, previousToken.orElse(null))
                    || each.getStartIndex() > previousToken.map(SQLToken::getStopIndex).orElse(0)) {
                appendRewriteSQL(each, result);
                previousToken = Optional.of(each);
            }
        }
        return result.toString();
    }
    
    private boolean isContainsAttachableToken(final SQLToken sqlToken, final SQLToken previousToken) {
        return sqlToken instanceof Attachable || previousToken instanceof Attachable;
    }
    
    private void appendRewriteSQL(final SQLToken sqlToken, final StringBuilder builder) {
        if (sqlToken instanceof ComposableSQLToken) {
            builder.append(getComposableSQLTokenText((ComposableSQLToken) sqlToken));
        } else {
            builder.append(getSQLTokenText(sqlToken));
        }
        builder.append(getConjunctionText(sqlToken));
    }
    
    protected abstract String getSQLTokenText(SQLToken sqlToken);
    
    private String getComposableSQLTokenText(final ComposableSQLToken composableSQLToken) {
        StringBuilder result = new StringBuilder();
        for (SQLToken each : composableSQLToken.getSqlTokens()) {
            result.append(getSQLTokenText(each));
            result.append(getConjunctionText(each));
        }
        return result.toString();
    }
    
    private String getConjunctionText(final SQLToken sqlToken) {
        int startIndex = getStartIndex(sqlToken);
        return sql.substring(startIndex, getStopIndex(sqlToken, startIndex));
    }
    
    private int getStartIndex(final SQLToken sqlToken) {
        int startIndex = sqlToken instanceof Substitutable ? ((Substitutable) sqlToken).getStopIndex() + 1 : sqlToken.getStartIndex();
        return Math.min(startIndex, sql.length());
    }
    
    private int getStopIndex(final SQLToken sqlToken, final int startIndex) {
        int currentSQLTokenIndex = sqlTokens.indexOf(sqlToken);
        int stopIndex = sqlTokens.size() - 1 == currentSQLTokenIndex ? sql.length() : sqlTokens.get(currentSQLTokenIndex + 1).getStartIndex();
        return startIndex <= stopIndex ? stopIndex : getStopIndex(sqlTokens.get(currentSQLTokenIndex + 1), startIndex);
    }
}
