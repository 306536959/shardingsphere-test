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

package org.apache.shardingsphere.mask.algorithm.hash;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.shardingsphere.mask.spi.MaskAlgorithm;

import java.util.Properties;

/**
 * MD5 mask algorithm.
 */
public final class SHA256MaskAlgorithm implements MaskAlgorithm<Object, String> {


    @Override
    public void init(final Properties props) {
        MaskAlgorithm.super.init(props);
    }

    @Override
    public String mask(final Object ciphertext) {
        if (null == ciphertext) {
            return null;
        }
        return DigestUtils.sha256Hex(String.valueOf(ciphertext));
    }

    public static void main(String[] args) {
        String ciphertext = "18972180945";
        String s = DigestUtils.sha256Hex(String.valueOf(ciphertext));
        System.out.println(s);
    }


    @Override
    public String getType() {
        return "SHA256";
    }
}
