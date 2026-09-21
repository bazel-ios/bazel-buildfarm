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

package build.buildfarm.instance.shard;

import static com.google.common.truth.Truth.assertThat;

import build.buildfarm.common.config.Backplane;
import build.buildfarm.common.config.BuildfarmConfigs;
import com.github.fppt.jedismock.RedisServer;
import com.github.fppt.jedismock.server.ServiceOptions;
import java.io.IOException;
import java.util.Collections;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

@RunWith(JUnit4.class)
public class OperationsTest {
  private static final int INVOCATION_TIMEOUT = 60;
  private final Backplane config = BuildfarmConfigs.getInstance().getBackplane();
  private RedisServer redisServer;
  private JedisCluster jedis;
  private Operations operations;
  private int previousInvocationTimeout;

  @Before
  public void setUp() throws IOException {
    previousInvocationTimeout = config.getMaxInvocationIdTimeout();
    config.setMaxInvocationIdTimeout(INVOCATION_TIMEOUT);
    redisServer =
        RedisServer.newRedisServer()
            .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled())
            .start();
    jedis =
        new JedisCluster(
            Collections.singleton(
                new HostAndPort(redisServer.getHost(), redisServer.getBindPort())));
    operations = new Operations("Operation", 120);
  }

  @After
  public void tearDown() throws IOException {
    config.setMaxInvocationIdTimeout(previousInvocationTimeout);
    if (jedis != null) {
      jedis.close();
    }
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  @Test
  public void defaultInvocationTimeoutIsOneWeek() {
    assertThat(new Backplane().getMaxInvocationIdTimeout()).isEqualTo(604800);
  }

  @Test
  public void emptyInvocationDoesNotCreateIndexButPreservesOperation() {
    operations.insert(jedis, new String(new char[0]), "op", "operation");

    assertThat(jedis.exists("")).isFalse();
    assertThat(operations.get(jedis, "op")).isEqualTo("operation");
    assertThat(jedis.ttl("Operation:op")).isGreaterThan(0L);
  }

  @Test
  public void emptyInvocationDoesNotGrowExistingIndex() {
    jedis.sadd("", "old-op");

    operations.insert(jedis, "", "op", "operation");

    assertThat(jedis.smembers("")).containsExactly("old-op");
    assertThat(operations.get(jedis, "op")).isEqualTo("operation");
  }

  @Test
  public void invocationIndexHasConfiguredExpiry() {
    operations.insert(jedis, "invocation", "op", "operation");

    assertThat(operations.getByInvocationId(jedis, "invocation")).containsExactly("op");
    assertThat(operations.get(jedis, "op")).isEqualTo("operation");
    assertInvocationExpires();
  }

  @Test
  public void newOperationExpiresExistingPersistentIndex() {
    jedis.sadd("invocation", "old-op");
    assertThat(jedis.ttl("invocation")).isEqualTo(-1L);

    operations.insert(jedis, "invocation", "op", "operation");

    assertThat(operations.getByInvocationId(jedis, "invocation")).containsExactly("old-op", "op");
    assertInvocationExpires();
  }

  @Test
  public void newOperationRefreshesInvocationExpiry() {
    operations.insert(jedis, "invocation", "first-op", "first");
    jedis.expire("invocation", 10);

    operations.insert(jedis, "invocation", "second-op", "second");

    assertThat(jedis.ttl("invocation")).isGreaterThan(10L);
    assertInvocationExpires();
  }

  private void assertInvocationExpires() {
    long ttl = jedis.ttl("invocation");
    assertThat(ttl).isGreaterThan(0L);
    assertThat(ttl).isAtMost((long) INVOCATION_TIMEOUT);
  }
}
