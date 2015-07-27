/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class MapKeysStep<S, E> extends FlatMapStep<S, E> {

    public MapKeysStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<S> traverser) {
        final S s = traverser.get();
        if (s instanceof Map)
            return ((Map) s).keySet().iterator();
        if (s instanceof Map.Entry)
            return Collections.singleton((E) ((Map.Entry) s).getKey()).iterator();
        return EmptyIterator.instance();
    }
}