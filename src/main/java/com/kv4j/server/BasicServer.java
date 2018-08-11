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
import com.kv4j.message.MessageHolder;
import com.kv4j.message.MessageReply;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public abstract class BasicServer implements Server {

    protected Disk disk = new Disk();

    protected State state = State.STOPPED;

    protected BlockingQueue<MessageHolder> mailBox = new LinkedBlockingDeque<>();

    public MessageReply send(Message msg) {
        MessageHolder holder = new MessageHolder(msg);
        mailBox.add(holder);
        return holder.getReply();
    }

    @Override
    public void replaceDisk(Disk disk) {
        this.disk = disk;
    }

    @Override
    public Disk getDisk() {
        return disk;
    }
}
