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

import com.kv4j.message.*;
import com.kv4j.server.BasicServer;
import com.kv4j.server.KV4jConfig;
import com.kv4j.server.ServerScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class Follower extends BasicServer {


    private final Logger logger = LogManager.getLogger(this.getClass());


    public Follower(String address) {
        super(address);
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

        this.state = State.RUNNING;

        scheduler.executor.submit(() -> {
            while(true) {
                try {
                    MessageHolder mh = mailBox.poll(KV4jConfig.CONFIG.HEARTBEAT_TIMEOUT, TimeUnit.SECONDS);
                    if (mh == null) {
                        // wait for random time and request vote
                        mh = mailBox.poll((long) (Math.random() * KV4jConfig.CONFIG.VOTE_WAIT_TIME), TimeUnit.MILLISECONDS);
                        // heartbeat timeout
                        // wait for random time and convert to candidate
                        if (mh == null) {
                            logger.warn(String.format("Follower %s timeout, convert To candidate", getAddress()));
                            state = State.STOPPED;
                            ServerScheduler.scheduler.convertTo(this, Type.CANDIDATE);
                            return;
                        }
                    }
                    Message message = mh.getMessage();
                    if (message instanceof AppendEntriesMessage) {
                        AppendEntriesMessage aeMsg = (AppendEntriesMessage) message;
                        if (aeMsg.isHeartbeat()) {
                            logger.trace("heartbeat..");
                            continue;
                        }
                        logger.info("server {} receive AppendEntriesMsg", getAddress());
                        //TODO
                    }
                    else if (message instanceof RequestVoteMessage) {
                        RequestVoteMessage vMsg = (RequestVoteMessage) message;
                        if (vMsg.getTerm() > curTerm()) {
                            this.setTerm(vMsg.getTerm());
                            mh.getReply().set(new VoteResponseMessage(true));
                        }
                        else {
                            mh.getReply().set(new VoteResponseMessage(false));
                        }
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

}
