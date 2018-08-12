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

import com.kv4j.message.UserMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Storage {
    public static class Disk {

        private Map<String,String> data = new HashMap<>();

        private int currentTerm;

        private String voteFor;

        private List<LogEntry> logs = new ArrayList<>(100);

        public Map<String, String> getData() {
            return data;
        }

        public int getCurrentTerm() {
            return currentTerm;
        }

        public void setCurrentTerm(int currentTerm) {
            this.currentTerm = currentTerm;
        }

        public String getVoteFor() {
            return voteFor;
        }

        public void setVoteFor(String voteFor) {
            this.voteFor = voteFor;
        }
    }

    public static class LogEntry {

        private final int term;

        private UserMessage userMessage;

        public LogEntry(int term, UserMessage userMessage) {
            this.term = term;
            this.userMessage = userMessage;
        }

        public int getTerm() {
            return term;
        }

        public UserMessage getUserMessage() {
            return userMessage;
        }
    }

    public static class Memory {

        private int commitedIdx;

        private int lastApplied;

        public int getCommitedIdx() {
            return commitedIdx;
        }

        public void setCommitedIdx(int commitedIdx) {
            this.commitedIdx = commitedIdx;
        }

        public int getLastApplied() {
            return lastApplied;
        }

        public void setLastApplied(int lastApplied) {
            this.lastApplied = lastApplied;
        }
    }
}
