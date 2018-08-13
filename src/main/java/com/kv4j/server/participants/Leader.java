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
import com.kv4j.message.UserMessage;
import com.kv4j.server.BasicServer;

import java.util.HashMap;
import java.util.Map;

public class Leader extends BasicServer {


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

    }

}
