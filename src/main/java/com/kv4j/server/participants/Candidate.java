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
import com.kv4j.message.MessageReply;
import com.kv4j.message.RequestVoteMessage;
import com.kv4j.server.BasicServer;
import com.kv4j.server.KV4jConfig;
import com.kv4j.server.ServerScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Candidate extends BasicServer {

    private Logger logger = LogManager.getLogger(this.getClass());

    private static ServerScheduler scheduler = ServerScheduler.scheduler;

    public Candidate(String address) {
        super(address);
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
        // start process for message handling
        scheduler.executor.submit(() -> {
            while(true) {
                MessageHolder mh = mailBox.poll(1, TimeUnit.SECONDS);
                if (mh == null) {
                    continue;
                }

                Message message = mh.getMessage();
                logger.info("got message<{}>", message.getClass());
            }
        });

        // start thread for vote process
        scheduler.executor.submit(() -> {
            // wait for random time and request vote
            try {
                Thread.sleep((long)(Math.random() * KV4jConfig.CONFIG.VOTE_WAIT_TIME));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            ReentrantLock sharedLock = new ReentrantLock();
            Condition sharedCondition = sharedLock.newCondition();
            List<MessageReply> replies = scheduler.broadcast(new RequestVoteMessage(), sharedLock, sharedCondition);
            while (true) {
                List<MessageReply> retReplies = MessageReply.selectReplies(replies, sharedLock, sharedCondition);
            }
        });
    }

}
