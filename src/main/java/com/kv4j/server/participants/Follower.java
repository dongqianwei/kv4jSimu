/*
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
package com.kv4j.server.participants;

import com.kv4j.message.Message;
import com.kv4j.message.MessageHolder;
import com.kv4j.server.BasicServer;
import com.kv4j.server.KV4jConfig;
import com.kv4j.server.Server;
import com.kv4j.server.scheduler.ServerScheduler;

import java.util.concurrent.TimeUnit;

public class Follower extends BasicServer {

    @Override
    public Message process(Message msg) {
        return null;
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public Type getType() {
        return Type.FOLLOWER;
    }

    @Override
    public void start() {

        ServerScheduler.scheduler.executor.submit(() -> {
            try {
                MessageHolder mh = mailBox.poll(KV4jConfig.CONFIG.HEARTBEAT_TIMEOUT, TimeUnit.SECONDS);
                // heartbeat timeout
                // wait for random time and convert to candidate
                if (mh == null) {
                    state = State.STOPPED;
                    ServerScheduler.scheduler.convertTo(this, Type.CANDIDATE);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
