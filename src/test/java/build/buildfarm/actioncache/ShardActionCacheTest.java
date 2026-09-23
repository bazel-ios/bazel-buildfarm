// Copyright 2026 The Bazel Authors. All rights reserved.
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

package build.buildfarm.actioncache;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.ActionKey;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class ShardActionCacheTest {
  private static final ActionKey ACTION_KEY =
      DigestUtil.asActionKey(Digest.newBuilder().setHash("action").setSizeBytes(1).build());

  private final AtomicLong nanos = new AtomicLong();
  private final Ticker ticker = nanos::get;
  private Backplane backplane;

  private ActionResult result(int exitCode) {
    return ActionResult.newBuilder().setExitCode(exitCode).build();
  }

  private ShardActionCache newCache(Duration expireAfterWrite) {
    return new ShardActionCache(
        10, expireAfterWrite, backplane, newDirectExecutorService(), ticker);
  }

  @Before
  public void setUp() throws Exception {
    backplane = mock(Backplane.class);
    when(backplane.getActionResult(ACTION_KEY)).thenReturn(result(0), result(1));
  }

  @Test
  public void expiredEntryIsReloadedFromBackplane() throws Exception {
    ShardActionCache cache = newCache(Duration.ofMinutes(10));

    assertThat(cache.get(ACTION_KEY).get()).isEqualTo(result(0));
    nanos.addAndGet(Duration.ofMinutes(9).toNanos());
    assertThat(cache.get(ACTION_KEY).get()).isEqualTo(result(0));
    verify(backplane, times(1)).getActionResult(ACTION_KEY);

    nanos.addAndGet(Duration.ofMinutes(2).toNanos());
    assertThat(cache.get(ACTION_KEY).get()).isEqualTo(result(1));
    verify(backplane, times(2)).getActionResult(ACTION_KEY);
  }

  @Test
  public void zeroExpiryKeepsEntryUntilInvalidated() throws Exception {
    ShardActionCache cache = newCache(Duration.ZERO);

    assertThat(cache.get(ACTION_KEY).get()).isEqualTo(result(0));
    nanos.addAndGet(Duration.ofDays(30).toNanos());
    assertThat(cache.get(ACTION_KEY).get()).isEqualTo(result(0));
    verify(backplane, times(1)).getActionResult(ACTION_KEY);

    cache.invalidate(ACTION_KEY);
    assertThat(cache.get(ACTION_KEY).get()).isEqualTo(result(1));
    verify(backplane, times(2)).getActionResult(ACTION_KEY);
  }
}
