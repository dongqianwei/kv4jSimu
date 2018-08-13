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
package com.kv4j.message;

import com.kv4j.server.KV4jCheckedException;
import com.kv4j.server.KV4jIllegalOperationException;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MessageReply {

    private AtomicReference<Message> replyRef = new AtomicReference<>();

    private ReentrantLock lock;
    private Condition condition;

    public MessageReply() {
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    public MessageReply(ReentrantLock lock, Condition condition) {
        this.lock = lock;
        this.condition = condition;
    }

    public Message get(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            while (replyRef.get() == null) {
                condition.await(timeout, unit);
            }
            return replyRef.get();
        }
        finally {
            lock.unlock();
        }
    }

    public Message get() {
        return replyRef.get();
    }


    public void set(Message reply) {
        lock.lock();
        try {
            if (!replyRef.compareAndSet(null, reply)) {
                throw new KV4jIllegalOperationException("MessageReply already set");
            }
            condition.signalAll();
        }
        finally {
            lock.unlock();
        }

    }

    public static Set<MessageReply> selectRepliesUnitl(
            Set<MessageReply> replies,
            ReentrantLock sharedLock,
            Condition sharedCondition, Date timeout) throws KV4jCheckedException {
        sharedLock.lock();
        try {
            while(true) {
                Set<MessageReply> ret = new HashSet<>();
                for (MessageReply reply : replies) {
                    if (reply.replyRef.get() != null) {
                        ret.add(reply);
                    }
                }
                if (ret.size() > 0) {
                    return ret;
                }
                else {
                    try {
                        if (!sharedCondition.awaitUntil(timeout)) {
                            throw new KV4jCheckedException();
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        finally {
            sharedLock.unlock();
        }
    }
}
