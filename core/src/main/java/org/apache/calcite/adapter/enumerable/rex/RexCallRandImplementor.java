/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.enumerable.rex;

import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

class RexCallRandImplementor extends RexCallAbstractImplementor {

  private final RexCallAbstractImplementor[] implementors = {
      new RexCallReflectiveImplementor(BuiltInMethod.RAND.method, nullPolicy),
      new RexCallReflectiveImplementor(BuiltInMethod.RAND_SEED.method, nullPolicy)
  };

  RexCallRandImplementor(NullPolicy nullPolicy) {
    super(nullPolicy);
  }

  @Override String getVariableName() {
    return "rand";
  }

  @Override Expression implementNotNull(final InnerVisitor translator,
      final RexCall call, final List<Expression> argValueList) {
    return implementors[call.getOperands().size()]
        .implementNotNull(translator, call, argValueList);
  }
}

// End RexCallRandImplementor.java
