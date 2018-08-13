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
package com.kv4j.client;

import com.kv4j.message.UserMessage;
import com.kv4j.server.ServerScheduler;

import java.util.Optional;

public class KV4jClient {

    private static ServerScheduler scheduler = ServerScheduler.scheduler;

    public void add(String key, String value) {
        scheduler.leaderAddr().ifPresent(s -> scheduler.sendMessage(s, UserMessage.makeAddMsg("hello", "world")));
    }

}
