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
import com.kv4j.message.MessageReply;
import com.kv4j.message.RequestVoteMessage;
import com.kv4j.server.BasicServer;
import com.kv4j.server.KV4jConfig;
import com.kv4j.server.Server;
import com.kv4j.server.scheduler.ServerScheduler;

import java.util.List;

public class Candidate extends BasicServer {

    private static ServerScheduler scheduler = ServerScheduler.scheduler;

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
        return Type.CANDIDATE;
    }

    @Override
    public void start() {
        scheduler.executor.submit(() -> {
            // wait for random time and request vote
            try {
                Thread.sleep(KV4jConfig.CONFIG.VOTE_WAIT_TIME);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            List<MessageReply> replies = scheduler.broadcast(new RequestVoteMessage());
        });
    }

}
