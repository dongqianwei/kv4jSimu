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

import com.kv4j.server.KV4jIllegalOperationException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class MessageReply {

    private AtomicReference<Message> replyRef = new AtomicReference<>();

    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    private static ReentrantLock sharedLock = new ReentrantLock();
    private static Condition sharedCondition = sharedLock.newCondition();

    private String fromAddress;

    public MessageReply(String fromAddress) {
        this.fromAddress = fromAddress;
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


    public void set(Message reply) {
        lock.lock();
        sharedLock.lock();
        try {
            if (!replyRef.compareAndSet(null, reply)) {
                throw new KV4jIllegalOperationException("MessageReply already set");
            }
            sharedCondition.signalAll();
            condition.signalAll();
        }
        finally {
            sharedLock.unlock();
            lock.unlock();
        }

    }

    public String getFromAddress() {
        return fromAddress;
    }

    public static List<MessageReply> selectReplies(List<MessageReply> replies) {
        sharedLock.lock();
        try {
            while(true) {
                List<MessageReply> ret = new ArrayList<>();
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
                        sharedCondition.await();
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
