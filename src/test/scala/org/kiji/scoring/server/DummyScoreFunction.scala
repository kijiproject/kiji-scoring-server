/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.scoring.server

import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.scoring.FreshenerContext
import org.kiji.scoring.ScoreFunction
import org.kiji.scoring.ScoreFunction.TimestampedValue

class DummyScoreFunction extends ScoreFunction[Int] {
  override def getDataRequest(
    context: FreshenerContext
  ): KijiDataRequest = {
    KijiDataRequest.create("info", "email")
  }

  override def score(
    dataToScore: KijiRowData,
    context: FreshenerContext
  ): TimestampedValue[Int] = {
    val param: String = context.getParameter("jennyanydots")
    if (null == param) {
      TimestampedValue.create(dataToScore.getMostRecentValue("info", "email").toString.length)
    } else {
      TimestampedValue.create(param.length)
    }
  }
}
