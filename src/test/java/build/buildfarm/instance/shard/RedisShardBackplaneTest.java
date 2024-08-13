// Copyright 2018 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.instance.shard.RedisShardBackplane.parseOperationChange;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Queue;
import build.buildfarm.common.redis.BalancedRedisQueue;
import build.buildfarm.common.redis.RedisClient;
import build.buildfarm.common.redis.RedisHashMap;
import build.buildfarm.common.redis.RedisMap;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.longrunning.Operation;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.UnifiedJedis;

@RunWith(JUnit4.class)
public class RedisShardBackplaneTest {
  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  @Mock Supplier<UnifiedJedis> mockJedisClusterFactory;

  @Before
  public void setUp() throws IOException {
    configs.getBackplane().setOperationExpire(10);
    configs.getBackplane().setQueues(new Queue[] {});
    MockitoAnnotations.initMocks(this);
  }

  public RedisShardBackplane createBackplane(String name) {
    return new RedisShardBackplane(
        name,
        /* subscribeToBackplane= */ false,
        /* runFailsafeOperation= */ false,
        o -> o,
        o -> o,
        mockJedisClusterFactory);
  }

  @Test
  public void workersWithInvalidProtobufAreRemoved() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    when(jedis.hgetAll(configs.getBackplane().getWorkersHashName() + "_storage"))
        .thenReturn(ImmutableMap.of("foo", "foo"));
    when(jedis.hdel(configs.getBackplane().getWorkersHashName() + "_storage", "foo"))
        .thenReturn(1L);
    RedisShardBackplane backplane = createBackplane("invalid-protobuf-worker-removed-test");
    backplane.start("startTime/test:0000");

    assertThat(backplane.getStorageWorkers()).isEmpty();
    verify(jedis, times(1)).hdel(configs.getBackplane().getWorkersHashName() + "_storage", "foo");
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedis, times(1))
        .publish(eq(configs.getBackplane().getWorkerChannel()), changeCaptor.capture());
    String json = changeCaptor.getValue();
    WorkerChange.Builder builder = WorkerChange.newBuilder();
    JsonFormat.parser().merge(json, builder);
    WorkerChange workerChange = builder.build();
    assertThat(workerChange.getName()).isEqualTo("foo");
    assertThat(workerChange.getTypeCase()).isEqualTo(WorkerChange.TypeCase.REMOVE);
  }

  OperationChange verifyChangePublished(String channel, UnifiedJedis jedis) throws IOException {
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedis, times(1)).publish(eq(channel), changeCaptor.capture());
    return parseOperationChange(changeCaptor.getValue());
  }

  String operationName(String name) {
    return "Operation:" + name;
  }

  @Test
  public void prequeueUpdatesOperationPrequeuesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.operations = mock(Operations.class);
    state.prequeue = mock(BalancedRedisQueue.class);
    RedisShardBackplane backplane = createBackplane("prequeue-operation-test");
    backplane.start(client, state, "startTime/test:0000");

    final String opName = "op";
    ExecuteEntry executeEntry = ExecuteEntry.newBuilder().setOperationName(opName).build();
    Operation op = Operation.newBuilder().setName(opName).build();
    backplane.prequeue(executeEntry, op);

    verify(state.operations, times(1))
        .insert(
            eq(jedis),
            any(String.class),
            eq(opName),
            eq(RedisShardBackplane.operationPrinter.print(op)));
    verifyNoMoreInteractions(state.operations);
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void queuingPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";
    backplane.queueing(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void requeueDispatchedOperationQueuesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.dispatchedOperations = mock(RedisHashMap.class);
    state.operationQueue = mock(OperationQueue.class);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start(client, state, "startTime/test:0000");

    final String opName = "op";
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(opName).build())
            .build();
    backplane.requeueDispatchedOperation(queueEntry);

    verify(state.dispatchedOperations, times(1)).remove(jedis, opName);
    verifyNoMoreInteractions(state.dispatchedOperations);
    verify(state.operationQueue, times(1))
        .push(
            jedis,
            queueEntry.getPlatform().getPropertiesList(),
            JsonFormat.printer().print(queueEntry),
            queueEntry.getExecuteEntry().getExecutionPolicy().getPriority());
    verifyNoMoreInteractions(state.operationQueue);
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount0to1()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 0;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 0;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 1;

    // create a backplane
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.dispatchedOperations = mock(RedisHashMap.class);
    state.dispatchingOperations = mock(RedisMap.class);
    state.operationQueue = mock(OperationQueue.class);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start(client, state, "startTime/test:0000");

    // ARRANGE
    // Assume the operation queue is already populated with a first-time operation.
    // this means the operation's requeue amount will be 0.
    // The jedis cluser is also mocked to assume success on other operations.
    final String opName = "op";
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(opName).build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    when(state.operationQueue.dequeue(eq(jedis), any(List.class))).thenReturn(queueEntryJson);
    when(state.operationQueue.removeFromDequeue(jedis, queueEntryJson)).thenReturn(true);
    // PRE-ASSERT
    when(state.dispatchedOperations.insertIfMissing(eq(jedis), eq(opName), any(String.class)))
        .thenAnswer(
            args -> {
              // Extract the operation that was dispatched
              String dispatchedOperationJson = args.getArgument(2);
              DispatchedOperation.Builder dispatchedOperationBuilder =
                  DispatchedOperation.newBuilder();
              JsonFormat.parser().merge(dispatchedOperationJson, dispatchedOperationBuilder);
              DispatchedOperation dispatchedOperation = dispatchedOperationBuilder.build();

              assertThat(dispatchedOperation.getQueueEntry().getRequeueAttempts())
                  .isEqualTo(REQUEUE_AMOUNT_WHEN_DISPATCHED);

              return true;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    QueueEntry readyForRequeue = backplane.dispatchOperation(ImmutableList.of());

    // ASSERT
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
    verify(state.operationQueue, times(1)).dequeue(eq(jedis), any(List.class));
    verify(state.operationQueue, times(1)).removeFromDequeue(jedis, queueEntryJson);
    verifyNoMoreInteractions(state.operationQueue);
    verify(state.dispatchedOperations, times(1))
        .insertIfMissing(
            eq(jedis), eq(queueEntry.getExecuteEntry().getOperationName()), any(String.class));
    verifyNoMoreInteractions(state.dispatchedOperations);
    verify(state.dispatchingOperations, times(1))
        .remove(jedis, queueEntry.getExecuteEntry().getOperationName());
    verifyNoMoreInteractions(state.dispatchingOperations);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount1to2()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 1;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 1;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 2;

    // create a backplane
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    RedisClient client = new RedisClient(jedis);
    DistributedState state = new DistributedState();
    state.dispatchedOperations = mock(RedisHashMap.class);
    state.dispatchingOperations = mock(RedisMap.class);
    state.operationQueue = mock(OperationQueue.class);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start(client, state, "startTime/test:0000");

    // ARRANGE
    // Assume the operation queue is already populated from a first re-queue.
    // this means the operation's requeue amount will be 1.
    // The jedis cluser is also mocked to assume success on other operations.
    final String opName = "op";
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName(opName).build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    when(state.operationQueue.dequeue(eq(jedis), any(List.class))).thenReturn(queueEntryJson);
    when(state.operationQueue.removeFromDequeue(jedis, queueEntryJson)).thenReturn(true);
    // PRE-ASSERT
    when(state.dispatchedOperations.insertIfMissing(eq(jedis), eq(opName), any(String.class)))
        .thenAnswer(
            args -> {
              // Extract the operation that was dispatched
              String dispatchedOperationJson = args.getArgument(2);
              DispatchedOperation.Builder dispatchedOperationBuilder =
                  DispatchedOperation.newBuilder();
              JsonFormat.parser().merge(dispatchedOperationJson, dispatchedOperationBuilder);
              DispatchedOperation dispatchedOperation = dispatchedOperationBuilder.build();

              assertThat(dispatchedOperation.getQueueEntry().getRequeueAttempts())
                  .isEqualTo(REQUEUE_AMOUNT_WHEN_DISPATCHED);

              return true;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    QueueEntry readyForRequeue = backplane.dispatchOperation(ImmutableList.of());

    // ASSERT
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
    verify(state.operationQueue, times(1)).dequeue(eq(jedis), any(List.class));
    verify(state.operationQueue, times(1)).removeFromDequeue(jedis, queueEntryJson);
    verifyNoMoreInteractions(state.operationQueue);
    verify(state.dispatchedOperations, times(1))
        .insertIfMissing(
            eq(jedis), eq(queueEntry.getExecuteEntry().getOperationName()), any(String.class));
    verifyNoMoreInteractions(state.dispatchedOperations);
    verify(state.dispatchingOperations, times(1))
        .remove(jedis, queueEntry.getExecuteEntry().getOperationName());
    verifyNoMoreInteractions(state.dispatchingOperations);
  }

  @Test
  public void completeOperationUndispatches() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("complete-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";

    backplane.completeOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName);
  }

  @Test
  @Ignore
  public void deleteOperationDeletesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("delete-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";

    backplane.deleteOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName);
    verify(jedis, times(1)).del(operationName(opName));
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void invocationsCanBeBlacklisted() throws IOException {
    UUID toolInvocationId = UUID.randomUUID();
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    String invocationBlacklistKey =
        configs.getBackplane().getInvocationBlacklistPrefix() + ":" + toolInvocationId;
    when(jedis.exists(invocationBlacklistKey)).thenReturn(true);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("invocation-blacklist-test");
    backplane.start("startTime/test:0000");

    assertThat(
            backplane.isBlacklisted(
                RequestMetadata.newBuilder()
                    .setToolInvocationId(toolInvocationId.toString())
                    .build()))
        .isTrue();

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).exists(invocationBlacklistKey);
  }

  @Test
  public void testGetWorkersStartTime() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("workers-starttime-test");
    backplane.start("startTime/test:0000");

    Set<String> workerNames = ImmutableSet.of("worker1", "worker2", "missing_worker");

    String storageWorkerKey = configs.getBackplane().getWorkersHashName() + "_storage";
    Map<String, String> workersJson =
        Map.of(
            "worker1",
                "{\"endpoint\": \"worker1\", \"expireAt\": \"9999999999999\", \"workerType\": 3,"
                    + " \"firstRegisteredAt\": \"1685292624000\"}",
            "worker2",
                "{\"endpoint\": \"worker2\", \"expireAt\": \"9999999999999\", \"workerType\": 3,"
                    + " \"firstRegisteredAt\": \"1685282624000\"}");
    when(jedis.hgetAll(storageWorkerKey)).thenReturn(workersJson);
    Map<String, Long> workersStartTime = backplane.getWorkersStartTimeInEpochSecs(workerNames);
    assertThat(workersStartTime.size()).isEqualTo(2);
    assertThat(workersStartTime.get("worker1")).isEqualTo(1685292624L);
    assertThat(workersStartTime.get("worker2")).isEqualTo(1685282624L);
    assertThat(workersStartTime.get("missing_worker")).isNull();
  }

  @Test
  public void getDigestInsertTime() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("digest-inserttime-test");
    backplane.start("startTime/test:0000");
    long ttl = 3600L;
    long expirationInSecs = configs.getBackplane().getCasExpire();
    when(jedis.ttl("ContentAddressableStorage:abc/0")).thenReturn(ttl);

    Digest digest = Digest.newBuilder().setHash("abc").build();

    Long insertTimeInSecs = backplane.getDigestInsertTime(digest);

    // Assuming there could be at most 2s delay in execution of both
    // `Instant.now().getEpochSecond()` call.
    assertThat(insertTimeInSecs)
        .isGreaterThan(Instant.now().getEpochSecond() - expirationInSecs + ttl - 2);
    assertThat(insertTimeInSecs).isAtMost(Instant.now().getEpochSecond() - expirationInSecs + ttl);
  }

  @Test
  public void testAddWorker() throws IOException {
    ShardWorker shardWorker =
        ShardWorker.newBuilder().setWorkerType(3).setFirstRegisteredAt(1703065913000L).build();
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    when(jedis.hset(anyString(), anyString(), anyString())).thenReturn(1L);
    RedisShardBackplane backplane = createBackplane("add-worker-test");
    backplane.start("addWorker/test:0000");
    backplane.addWorker(shardWorker);
    verify(jedis, times(1))
        .hset(
            configs.getBackplane().getWorkersHashName() + "_storage",
            "",
            JsonFormat.printer().print(shardWorker));
    verify(jedis, times(1))
        .hset(
            configs.getBackplane().getWorkersHashName() + "_execute",
            "",
            JsonFormat.printer().print(shardWorker));
    verify(jedis, times(1)).publish(anyString(), anyString());
  }
}
