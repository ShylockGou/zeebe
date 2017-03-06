/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.tngp.client.impl.cmd.taskqueue;

public class TaskSubscription
{
    private long id;

    private long topicId;
    private String taskType;

    private long lockDuration;
    private long lockOwner;
    private int credits;

    public long getId()
    {
        return id;
    }

    public void setId(long id)
    {
        this.id = id;
    }

    public long getTopicId()
    {
        return topicId;
    }

    public void setTopicId(long topicId)
    {
        this.topicId = topicId;
    }

    public String getTaskType()
    {
        return taskType;
    }

    public void setTaskType(String taskType)
    {
        this.taskType = taskType;
    }

    public long getLockDuration()
    {
        return lockDuration;
    }

    public void setLockDuration(long lockDuration)
    {
        this.lockDuration = lockDuration;
    }

    public int getCredits()
    {
        return credits;
    }

    public void setCredits(int credits)
    {
        this.credits = credits;
    }

    public long getLockOwner()
    {
        return lockOwner;
    }

    public void setLockOwner(long lockOwner)
    {
        this.lockOwner = lockOwner;
    }

}