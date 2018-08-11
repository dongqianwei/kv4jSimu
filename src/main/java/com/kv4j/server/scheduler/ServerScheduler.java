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
package com.kv4j.server.scheduler;

import com.kv4j.message.Message;
import com.kv4j.message.MessageHolder;
import com.kv4j.message.MessageReply;
import com.kv4j.server.BasicServer;
import com.kv4j.server.participants.Candidate;
import com.kv4j.server.participants.Follower;
import com.kv4j.server.Server;
import com.kv4j.server.participants.Leader;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ServerScheduler {


    public static ServerScheduler scheduler = new ServerScheduler();

    private ServerScheduler() {
        //
    }

    public void convertTo(Server server, Server.Type type) {
        for (String id : servers.keySet()) {
            if (servers.get(id) == server) {
                Server newServer = null;
                switch (type) {
                    case LEADER:
                        newServer = new Leader();
                        break;
                    case FOLLOWER:
                        newServer = new Follower();
                        break;
                    case CANDIDATE:
                        newServer = new Candidate();
                }
                newServer.replaceDisk(server.getDisk());
                servers.put(id, newServer);
                newServer.start();
            }
        }
    }

    private Map<String, Server> servers = new ConcurrentHashMap<>();

    public ExecutorService executor = Executors.newCachedThreadPool();


    public void start(int initServerNum) {
        for (int i = 0; i < initServerNum ; i++) {
            String id = UUID.randomUUID().toString();
            Server server = new Follower();
            servers.put(id, server);
            server.start();
        }
    }

    public List<MessageReply> broadcast(Message message) {
        List<MessageReply> msgs = new ArrayList<>();
        servers.forEach((id, server) -> {
            msgs.add(server.send(message));
        });
        return msgs;
    }

}
