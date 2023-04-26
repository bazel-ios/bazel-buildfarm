// Copyright 2023 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package build.buildfarm.worker;

import com.google.common.util.concurrent.SettableFuture;
import java.util.logging.Level;
import lombok.extern.java.Log;
import java.util.Set;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.management.ThreadInfo;

@Log
public class PipelineStageThreadGroup extends ThreadGroup {
  private SettableFuture<Void> uncaughtExceptionFuture = null;

  public PipelineStageThreadGroup() {
    super("pipeline-stages");
  }

  public void setUncaughtExceptionFuture(SettableFuture<Void> uncaughtExceptionFuture) {
    this.uncaughtExceptionFuture = uncaughtExceptionFuture;
  }

  public void dumpAllThreads() {
    // This is a similar device of calling jstack on the pid but we do it at
    // the precisce moment, and here prior to interrupting pipeline threads
    System.err.println("SEVERE: PipelineStageThreadGroup BEGIN uncaught stack trace");
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] infos = bean.dumpAllThreads(true, true);
    String errs = Arrays.stream(infos).map(Object::toString)
	.collect(Collectors.joining());
    System.err.println(errs);
    System.err.println("SEVERE: PipelineStageThreadGroup END uncaught stack trace");
  }


  // If there is an uncaught exception in the thread group, interrupt
  // stage threads and notify the caller to decide how to handle it
  @Override
  public void uncaughtException(Thread caughtThread, Throwable exception) {
    // This will catch any uncaught exception in a pipeline. Include the thread
    // name to further identifty failing sub-systems

    log.log(
        Level.SEVERE,
        String.format(
            "PipelineStage thread %s: terminating due to uncaught exception",
            caughtThread.getName()),
        exception);
    // TODO: probably put this behind a flag..
    dumpAllThreads();

    interrupt();

    if (uncaughtExceptionFuture != null) {
      uncaughtExceptionFuture.setException(exception);
    }
  }
}
