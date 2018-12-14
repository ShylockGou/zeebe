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
package io.zeebe.broker.it.workflow;

import static io.zeebe.protocol.intent.WorkflowInstanceIntent.EVENT_ACTIVATED;
import static io.zeebe.protocol.intent.WorkflowInstanceIntent.EVENT_TRIGGERING;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.PartitionTestClient;
import io.zeebe.test.util.TestUtil;
import io.zeebe.test.util.record.RecordingExporter;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class TimerStartEventTest {

  public static final BpmnModelInstance SIMPLE_MODEL =
      Bpmn.createExecutableProcess("process")
          .startEvent("start_1")
          .timerWithCycle("R1/PT1S")
          .endEvent("end_1")
          .done();

  public static final BpmnModelInstance REPEATING_MODEL =
      Bpmn.createExecutableProcess("process")
          .startEvent("start_2")
          .timerWithCycle("R/PT1S")
          .endEvent("end_2")
          .done();

  public static final BpmnModelInstance THREE_SEC_MODEL =
      Bpmn.createExecutableProcess("process_3")
          .startEvent("start_3")
          .timerWithCycle("R2/PT3S")
          .endEvent("end_3")
          .done();

  public EmbeddedBrokerRule brokerRule =
      new EmbeddedBrokerRule(
          brokerCfg -> {
            final ExporterCfg exporterCfg = new ExporterCfg();
            exporterCfg.setClassName(RecordingExporter.class.getCanonicalName());
            exporterCfg.setId("test-exporter");
            brokerCfg.setExporters(Collections.singletonList(exporterCfg));
          });

  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  private PartitionTestClient testClient;

  @Before
  public void setUp() {
    testClient = apiRule.partitionClient();
  }

  @Rule public Timeout timeoutRule = new Timeout(2, TimeUnit.MINUTES);

  @Test
  public void shouldDeployWorkflow() {
    // when
    final long deploymentKey = testClient.deploy(SIMPLE_MODEL);

    // then
    assertThat(deploymentKey).isGreaterThan(-1);
  }

  @Test
  public void shouldCreateTimer() {
    // when
    testClient.deploy(SIMPLE_MODEL);
    TestUtil.waitUntil(() -> RecordingExporter.timerRecords(TimerIntent.CREATED).count() == 1);

    // then
    assertThat(RecordingExporter.deploymentRecords(DeploymentIntent.CREATED)).hasSize(1);
  }

  @Test
  public void shouldTriggerAndCreateWorkflowInstance() {
    // when
    testClient.deploy(SIMPLE_MODEL);
    TestUtil.waitUntil(() -> RecordingExporter.timerRecords(TimerIntent.CREATED).count() == 1);
    brokerRule.getClock().addTime(Duration.ofSeconds(2));

    // then
    assertThat(
            RecordingExporter.workflowInstanceRecords()
                .withIntent(WorkflowInstanceIntent.ELEMENT_READY)
                .limit(1))
        .hasSize(1);
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).limit(2)).hasSize(1);
    assertThat(RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.CREATE).limit(2))
        .hasSize(1);
  }

  @Test
  public void shouldCreateMultipleWorkflowInstancesWithRepeatingTimer() {
    // when
    testClient.deploy(THREE_SEC_MODEL);

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).limit(1)).hasSize(1);
    brokerRule.getClock().addTime(Duration.ofSeconds(4));

    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).limit(1)).hasSize(1);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .limit(1))
        .hasSize(1);

    brokerRule.getClock().addTime(Duration.ofSeconds(4));
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(2);
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(2);

    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .limit(2))
        .hasSize(2);
  }

  @Test
  public void shouldCompleteWorkflow() {
    // when
    brokerRule.getClock().pinCurrentTime();
    testClient.deploy(SIMPLE_MODEL);

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(1);
    brokerRule.getClock().addTime(Duration.ofSeconds(6));

    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);

    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY).count())
        .isEqualTo(1);

    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.EVENT_TRIGGERED)
                .withElementId("start_1")
                .count())
        .isEqualTo(1);

    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.EVENT_ACTIVATED)
                .withElementId("end_1")
                .count())
        .isEqualTo(1);
  }

  @Test
  public void shouldUpdateWorkflow() {
    // when
    brokerRule.getClock().pinCurrentTime();
    testClient.deploy(SIMPLE_MODEL);

    TestUtil.waitUntil(() -> RecordingExporter.timerRecords(TimerIntent.CREATED).count() == 1);
    brokerRule.getClock().addTime(Duration.ofSeconds(2));

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);
    final WorkflowInstanceRecordValue firstInstance =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.CREATE)
            .limit(1)
            .getFirst()
            .getValue();

    assertThat(firstInstance.getVersion()).isEqualTo(1);
    assertThat(firstInstance.getBpmnProcessId()).isEqualTo("process");
    assertThat(
            RecordingExporter.workflowInstanceRecords(EVENT_ACTIVATED)
                .withElementId("end_1")
                .count())
        .isEqualTo(1);

    // when
    testClient.deploy(REPEATING_MODEL);

    TestUtil.waitUntil(
        () -> RecordingExporter.timerRecords(TimerIntent.CREATED).limit(2).count() == 2);
    brokerRule.getClock().addTime(Duration.ofSeconds(2));

    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(2);

    final WorkflowInstanceRecordValue secondInstance =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.CREATE)
            .limit(2)
            .getLast()
            .getValue();

    assertThat(secondInstance.getVersion()).isEqualTo(2);
    assertThat(secondInstance.getBpmnProcessId()).isEqualTo("process");
    assertThat(
            RecordingExporter.workflowInstanceRecords(EVENT_ACTIVATED)
                .withElementId("end_2")
                .count())
        .isEqualTo(1);
  }

  @Test
  public void shouldReplaceTimerStartWithNoneStart() {
    // when
    brokerRule.getClock().pinCurrentTime();
    testClient.deploy(REPEATING_MODEL);
    TestUtil.waitUntil(() -> RecordingExporter.timerRecords(TimerIntent.CREATED).count() == 1);
    brokerRule.getClock().addTime(Duration.ofSeconds(2));

    // then
    final WorkflowInstanceRecordValue firstInstance =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.CREATE)
            .limit(1)
            .getFirst()
            .getValue();

    assertThat(firstInstance.getVersion()).isEqualTo(1);
    assertThat(
            RecordingExporter.workflowInstanceRecords(EVENT_TRIGGERING)
                .withElementId("start_2")
                .count())
        .isEqualTo(1);

    // when
    final BpmnModelInstance nonTimerModel =
        Bpmn.createExecutableProcess("process").startEvent("start_4").endEvent("end_4").done();
    testClient.deploy(nonTimerModel);

    TestUtil.waitUntil(
        () -> RecordingExporter.deploymentRecords(DeploymentIntent.CREATED).count() == 2);

    brokerRule.getClock().addTime(Duration.ofSeconds(2));
    final long workflowInstanceKey = testClient.createWorkflowInstance("process");

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);
    assertThat(workflowInstanceKey).isGreaterThan(0);
    final WorkflowInstanceRecordValue secondInstance =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
            .limit(2)
            .getLast()
            .getValue();
    assertThat(secondInstance.getVersion()).isEqualTo(2);
    assertThat(
            RecordingExporter.workflowInstanceRecords(EVENT_TRIGGERING)
                .withElementId("start_4")
                .count())
        .isEqualTo(1);

    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);
  }

  @Test
  public void shouldUpdateTimerPeriod() {
    // when
    brokerRule.getClock().pinCurrentTime();
    testClient.deploy(THREE_SEC_MODEL);

    TestUtil.waitUntil(() -> RecordingExporter.timerRecords(TimerIntent.CREATED).count() == 1);
    brokerRule.getClock().addTime(Duration.ofSeconds(3));

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(1);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.EVENT_TRIGGERED)
                .withElementId("start_3")
                .count())
        .isEqualTo(1);

    // when
    final BpmnModelInstance slowerModel =
        Bpmn.createExecutableProcess("process_3")
            .startEvent("start_4")
            .timerWithCycle("R2/PT6S")
            .endEvent("end_4")
            .done();

    testClient.deploy(slowerModel);
    TestUtil.waitUntil(
        () -> RecordingExporter.deploymentRecords(DeploymentIntent.CREATED).count() == 2);
    brokerRule.getClock().addTime(Duration.ofSeconds(4));

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);

    brokerRule.getClock().addTime(Duration.ofSeconds(3));
    TestUtil.waitUntil(() -> RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count() == 2);

    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(2);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.EVENT_TRIGGERED)
                .withElementId("start_4")
                .count())
        .isEqualTo(1);
  }

  @Test
  public void shouldTriggerDifferentWorkflowsSeparately() {
    // when
    brokerRule.getClock().pinCurrentTime();
    testClient.deploy(THREE_SEC_MODEL);
    testClient.deploy(REPEATING_MODEL);

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(2);

    brokerRule.getClock().addTime(Duration.ofSeconds(1));

    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).limit(1)).hasSize(1);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process")
                .count())
        .isEqualTo(1);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(0);

    brokerRule.getClock().addTime(Duration.ofSeconds(1));
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process")
                .count())
        .isEqualTo(2);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(0);

    brokerRule.getClock().addTime(Duration.ofSeconds(1));
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process")
                .count())
        .isEqualTo(3);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(1);
  }
}
