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
package com.hortonworks.registries.common.ha;

import java.io.IOException;

/**
 *
 */
public interface LeadershipClient {
    LeadershipClient LOCAL_LEADER = new LeadershipClient() {
        @Override
        public void participateForLeadership() throws Exception {
        }

        @Override
        public String getCurrentLeader() throws Exception {
            return null;
        }

        @Override
        public void exitFromLeaderParticipation() throws IOException {

        }

        @Override
        public boolean hasLeadership() {
            return true;
        }

        @Override
        public void close() throws IOException {
        }
    };


    /**
     * Participates for leader lock with the given configuration.
     *
     * @throws Exception if any errors encountered.
     */
    void participateForLeadership() throws Exception;

    /**
     * Returns the current leader's participant id.
     * @throws Exception if any error occurs.
     */
    String getCurrentLeader() throws Exception;

    /**
     * Exits the current leader latch by closing it. Thi smay thro an Exception if the current latch is not yet started
     * or it has already been closed.
     *
     * @throws IOException if any IO related errors occur.
     */
    void exitFromLeaderParticipation() throws IOException;

    /**
     * Returns true if the current participant is a leader
     */
    boolean hasLeadership();

    /**
     * Closes the underlying ZK client resources.
     *
     * @throws IOException if any io errors occurred during this operation.
     */
    void close() throws IOException;
}
