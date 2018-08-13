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
import com.kv4j.server.KV4jIllegalOperationException;
import com.kv4j.server.ServerScheduler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Candidate extends BasicServer {

    private Logger logger = LogManager.getLogger(this.getClass());

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

        this.state = State.RUNNING;
        // start process for message handling
        scheduler.executor.submit(() -> {
            while(true) {
                MessageHolder mh = mailBox.poll(1, TimeUnit.SECONDS);
                if (mh == null) {
                    continue;
                }

                Message message = mh.getMessage();
                logger.info("got message<{}>", message.getClass());

                if (message instanceof UserMessage) {
                    throw new KV4jIllegalOperationException("UserMessage Not Allowed here!");
                }

                if (message instanceof RaftMessage) {
                    RaftMessage rMsg = (RaftMessage) message;
                    // if msg term > curTerm
                    // convert to Follower
                    if (rMsg.getTerm() > curTerm()) {
                        this.setTerm(rMsg.getTerm());
                        this.state = State.STOPPED;
                        scheduler.convertTo(this, Type.FOLLOWER);
                    }
                }
            }
        });

        // start thread for vote process
        scheduler.executor.submit(() -> {
            int numGranted = 0;
            ReentrantLock sharedLock = new ReentrantLock();
            Condition sharedCondition = sharedLock.newCondition();
            Set<MessageReply> replies = scheduler.broadcast(new RequestVoteMessage()
                    .setTerm(curTerm())
                    .incTerm()
                    .setFromAddress(getAddress()), sharedLock, sharedCondition);
            while (true) {
                Set<MessageReply> retReplies = MessageReply.selectReplies(replies, sharedLock, sharedCondition);
                for (MessageReply r: retReplies) {
                    replies.remove(r);
                    if (!(r.get() instanceof VoteResponseMessage)) {
                        throw new KV4jIllegalOperationException("requestVote response type error: " + r.get().getClass());
                    }

                    VoteResponseMessage vrMsg = (VoteResponseMessage) r.get();
                    if (vrMsg.isGranted()) {
                        numGranted ++;
                    }

                    // get majority votes, become leader
                    if (numGranted > scheduler.majorityServerNum()) {
                        this.state = State.STOPPED;
                        scheduler.convertTo(this, Type.LEADER);
                        return;
                    }

                    if (replies.size() == 0) {
                        logger.warn("failed to got more than half votes");
                    }
                }
            }
        });
    }

}
