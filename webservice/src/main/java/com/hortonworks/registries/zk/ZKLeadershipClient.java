/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.registries.zk;

import com.hortonworks.registries.common.ha.LeadershipClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class ZKLeadershipClient implements LeadershipClient {

    public static final String LEADER_LOCK_NODE_NAME = "leader-lock";
    public static final int DEFAULT_CONN_TIMOUT = 20_1000;
    public static final int DEFAULT_SESSION_TIMEOUT = 30_1000;
    public static final int DEFAULT_BASE_SLEEP_TIME = 1000;
    public static final int DEFAULT_MAX_SLEEP_TIME = 5000;
    public static final int DEFAULT_RETRY_LIMIT = 5;

    private static final Logger LOG = LoggerFactory.getLogger(ZKLeadershipClient.class);

    private final CuratorFramework curatorFramework;
    private final Map<String, Object> conf;
    private final String serverUrl;
    private final LeaderLatchListener leaderLatchListener;
    private final String latchPath;
    private final AtomicReference<LeaderLatch> leaderLatchRef;

    public ZKLeadershipClient(Map<String, Object> conf, String serverUrl) {
        this.conf = conf;
        this.serverUrl = serverUrl;
        this.leaderLatchListener = createLeaderLatchListener();

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

        String url = (String) conf.get("connect.url");
        String root = (String) conf.get("root");
        builder.connectString(url + root);
        builder.connectionTimeoutMs((Integer) conf.getOrDefault("connection.timeout.ms", DEFAULT_CONN_TIMOUT));
        builder.sessionTimeoutMs((Integer) conf.getOrDefault("session.timeout.ms", DEFAULT_SESSION_TIMEOUT));

        builder.retryPolicy(
                new BoundedExponentialBackoffRetry(
                        (Integer) conf.getOrDefault("retry.base.sleep.time.ms", DEFAULT_BASE_SLEEP_TIME),
                        (Integer) conf.getOrDefault("retry.max.sleep.time.ms", DEFAULT_MAX_SLEEP_TIME),
                        (Integer) conf.getOrDefault("retry.limit", DEFAULT_RETRY_LIMIT)

                ));

        curatorFramework = builder.build();
        latchPath = conf.getOrDefault("zookeeper.root", "registry") + LEADER_LOCK_NODE_NAME;
        leaderLatchRef = new AtomicReference<>(createLeaderLatch());
    }

    private LeaderLatchListener createLeaderLatchListener() {
        return new LeaderLatchListener() {
            @Override
            public void isLeader() {
                LOG.info("This instance acquired leadership");
            }

            @Override
            public void notLeader() {
                LOG.info("This instance lost leadership");
            }
        };
    }

    private LeaderLatch createLeaderLatch() {
        return new LeaderLatch(curatorFramework, latchPath, serverUrl);
    }

    /**
     * Participates for leader lock with the given configuration.
     *
     * @throws Exception if any errors encountered.
     */
    @Override
    public void participateForLeadership() throws Exception {
        // if the existing leader latch is closed, recreate and connect again
        if (LeaderLatch.State.CLOSED.equals(leaderLatchRef.get().getState())) {
            // remove listener from earlier closed leader latch
            leaderLatchRef.get().removeListener(leaderLatchListener);

            leaderLatchRef.set(createLeaderLatch());
            leaderLatchRef.get().addListener(leaderLatchListener);
            LOG.info("Existing leader latch is in CLOSED state, it is recreated.");
        }

        // if the existing leader latch is not yet started, start now!!
        if (LeaderLatch.State.LATENT.equals(leaderLatchRef.get().getState())) {
            leaderLatchRef.get().start();
            LOG.info("Existing leader latch is in LATENT state, it is started.");
        }
    }

    /**
     * Returns the current leader's participant id.
     * @throws Exception if any error occurs.
     */
    @Override
    public String getCurrentLeader() throws Exception {
        return leaderLatchRef.get().getLeader().getId();
    }

    /**
     * Exits the current leader latch by closing it. This may throw an Exception if the current latch is not yet started
     * or it has already been closed.
     *
     * @throws IOException if any IO related errors occur.
     */
    @Override
    public void exitFromLeaderParticipation() throws IOException {
        // close the current leader latch for removing from leader participation.
        leaderLatchRef.get().close();
    }

    /**
     * Returns true if the current participant is a leader
     */
    @Override
    public boolean hasLeadership() {
        return leaderLatchRef.get().hasLeadership();
    }

    /**
     * Closes the underlying ZK client resources.
     *
     * @throws IOException if any io errors occurred during this operation.
     */
    @Override
    public void close() throws IOException {
        curatorFramework.close();
    }
}
