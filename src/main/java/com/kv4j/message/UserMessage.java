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

public class UserMessage extends BasicMessage {

    public enum MsgType {
        ADD,
        DELETE,
        UPDATE
    }

    private MsgType type;
    private String key;
    private String value;

    private UserMessage(MsgType type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static UserMessage makeAddMsg(String key, String value) {
        return new UserMessage(MsgType.ADD, key, value);
    }

    public static UserMessage makeUpdateMsg(String key, String value) {
        return new UserMessage(MsgType.UPDATE, key, value);
    }

    public static UserMessage makeDeleteMsg(String key) {
        return new UserMessage(MsgType.DELETE, key, null);
    }

    public MsgType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "UserMessage{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
