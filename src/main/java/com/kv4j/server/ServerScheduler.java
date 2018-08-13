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
package com.kv4j.server;

import com.kv4j.message.Message;
import com.kv4j.message.MessageReply;
import com.kv4j.server.participants.Candidate;
import com.kv4j.server.participants.Follower;
import com.kv4j.server.participants.Leader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ServerScheduler {


    private final Logger logger = LogManager.getLogger(this.getClass());


    public static ServerScheduler scheduler = new ServerScheduler();

    private int serverNum = 0;

    private ServerScheduler() {
        //
    }

    private AtomicReference<String> leaderAddrRef = new AtomicReference<>();

    public Optional<String> leaderAddr() {
        return Optional.ofNullable(leaderAddrRef.get());
    }

    public void convertTo(Server server, Server.Type type) {
        for (String addr : servers.keySet()) {
            if (servers.get(addr) == server) {
                logger.info("server {} convert from {} to {}", addr, server.getType(), type);
                Server newServer = null;
                switch (type) {
                    case LEADER:
                        newServer = new Leader(addr);
                        break;
                    case FOLLOWER:
                        newServer = new Follower(addr);
                        break;
                    case CANDIDATE:
                        newServer = new Candidate(addr);
                }
                newServer.replaceStorage(server.getStorage());
                servers.put(addr, newServer);
                newServer.start();
                if (type == Server.Type.LEADER) {
                    leaderAddrRef.set(addr);
                }
            }
        }
    }

    private Map<String, Server> servers = new ConcurrentHashMap<>();

    public ExecutorService executor = Executors.newCachedThreadPool();


    public void start(int initServerNum) {
        for (int i = 0; i < initServerNum ; i++) {
            String id = UUID.randomUUID().toString();
            Server server = new Follower(id);
            servers.put(id, server);
            server.start();
            serverNum ++;
        }
    }

    public int serverNum() {
        return serverNum;
    }

    public int majorityServerNum() {
        return serverNum / 2 + 1;
    }

    public void broadcast(Message message) {
        servers.forEach((id, server) -> {
            if (message.getFromAddress().equals(id)) {
                return;
            }
            server.send(message.setToAddress(id));
        });
    }

    public Set<MessageReply> broadcast(Message message, ReentrantLock lock, Condition condition) {
        Set<MessageReply> msgs = new HashSet<>();
        servers.forEach((id, server) -> {
            if (message.getFromAddress().equals(id)) {
                return;
            }
            msgs.add(server.send(message.setToAddress(id), lock, condition));
        });
        return msgs;
    }

    public MessageReply sendMessage(String address, Message message) {
        message.setToAddress(address);
        Server server = servers.get(address);
        if (server == null) {
            throw new KV4jIllegalOperationException(String.format("server %s does not exist", address));
        }
        return server.send(message);
    }

}
