/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
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
package io.zeebe.broker.client.job.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.zeebe.broker.client.impl.subscription.SubscriberGroups;
import io.zeebe.broker.client.impl.subscription.SubscriptionExecutor;
import io.zeebe.broker.client.impl.subscription.job.JobSubscriberGroup;
import org.junit.Test;

public class JobExecutorTest {
  @Test
  public void shouldExecuteJobs() throws Exception {
    // given
    final SubscriberGroups subscriptions = new SubscriberGroups();

    final JobSubscriberGroup subscription = mock(JobSubscriberGroup.class);
    when(subscription.poll()).thenReturn(34);
    subscriptions.addGroup(subscription);

    final SubscriptionExecutor executor =
        new SubscriptionExecutor(subscriptions, new SubscriberGroups());

    // when
    final int workCount = executor.doWork();

    // then
    assertThat(workCount).isEqualTo(34);

    verify(subscription).poll();
  }
}