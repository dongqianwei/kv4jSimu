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

import com.kv4j.message.AppendEntriesMessage;
import com.kv4j.message.Message;
import com.kv4j.message.MessageHolder;
import com.kv4j.message.UserMessage;
import com.kv4j.server.BasicServer;
import com.kv4j.server.KV4jConfig;
import com.kv4j.server.ServerScheduler;
import com.kv4j.server.Storage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Leader extends BasicServer {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(1);

    private ServerScheduler scheduler = ServerScheduler.scheduler;

    private Map<String, Integer> nextIdx = new HashMap<>();

    private Map<String, Integer> matchedIdx = new HashMap<>();

    public Leader(String address) {
        super(address);
    }

    @Override
    public State getState() {
        return state;
    }

    @Override
    public Type getType() {
        return Type.LEADER;
    }

    @Override
    public void start() {
        initLeader();
        this.state = State.RUNNING;
        // heartbeat thread
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            scheduler.broadcast(new AppendEntriesMessage()
                    .setTerm(curTerm())
                    .setFromAddress(getAddress()));
        }, 0, KV4jConfig.CONFIG.HEARTBEAT, TimeUnit.SECONDS);

        // process message
        scheduler.executor.submit(() -> {
            MessageHolder tmpMH = null;
            try {
                while(state == State.RUNNING) {
                    while (tmpMH == null) {
                        tmpMH = mailBox.poll(5, TimeUnit.SECONDS);
                        if (state != State.RUNNING) {
                            return;
                        }
                    }

                    MessageHolder mh = tmpMH;
                    tmpMH = null;

                    Message message = mh.getMessage();
                    if (message instanceof UserMessage) {
                        UserMessage userMsg = (UserMessage) message;
                        logger.info("LEADER recv user message {}", message);
                        addLogEntry(userMsg);
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void addLogEntry(UserMessage msg) {
        this.getStorage().getDisk().getLogs().add(new Storage.LogEntry(curTerm(), msg));
    }

    private void initLeader() {
        List<String> addrList = scheduler.getAddrList();
        for (String addr : addrList) {
            nextIdx.put(addr, this.lastLogIdx() + 1);
            matchedIdx.put(addr, 0);
        }
    }
}
