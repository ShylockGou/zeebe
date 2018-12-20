/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.workflow;

import static io.zeebe.protocol.intent.WorkflowInstanceIntent.EVENT_ACTIVATED;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.broker.workflow.deployment.distribute.processor.DeploymentCreatedProcessor;
import io.zeebe.exporter.record.Assertions;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.TimerRecordValue;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.ExecuteCommandResponse;
import io.zeebe.test.broker.protocol.clientapi.PartitionTestClient;
import io.zeebe.test.util.record.RecordingExporter;
import java.time.Duration;
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

  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  private PartitionTestClient testClient;

  @Before
  public void setUp() {
    brokerRule.getClock().pinCurrentTime();
    testClient = apiRule.partitionClient();
  }

  @Rule public Timeout timeoutRule = new Timeout(2, TimeUnit.MINUTES);

  @Test
  public void shouldCreateTimer() {
    // when
    testClient.deploy(SIMPLE_MODEL);

    // then
    Assertions.assertThat(RecordingExporter.deploymentRecords(DeploymentIntent.CREATED).getFirst())
        .isNotNull();
    final TimerRecordValue timerRecord =
        RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst().getValue();

    Assertions.assertThat(timerRecord)
        .hasDueDate(brokerRule.getClock().getCurrentTimeInMillis() + 1000);
    Assertions.assertThat(timerRecord).hasHandlerFlowNodeId("start_1");
    Assertions.assertThat(timerRecord)
        .hasElementInstanceKey(DeploymentCreatedProcessor.NO_ELEMENT_INSTANCE);
  }

  @Test
  public void shouldTriggerAndCreateWorkflowInstance() {
    // when
    final ExecuteCommandResponse response = testClient.deployWithResponse(SIMPLE_MODEL);
    final DeploymentRecordValue deploymentRecord =
        testClient
            .receiveFirstDeploymentEvent(DeploymentIntent.CREATED, response.getKey())
            .getValue();

    // then
    Assertions.assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst())
        .isNotNull();
    brokerRule.getClock().addTime(Duration.ofSeconds(2));

    Assertions.assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).getFirst())
        .isNotNull();
    Assertions.assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.CREATE).getFirst())
        .isNotNull();

    final WorkflowInstanceRecordValue wfInstanceRecord =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
            .getFirst()
            .getValue();

    Assertions.assertThat(wfInstanceRecord).hasVersion(1);
    Assertions.assertThat(wfInstanceRecord).hasBpmnProcessId("process");
    Assertions.assertThat(wfInstanceRecord)
        .hasWorkflowKey(deploymentRecord.getDeployedWorkflows().get(0).getWorkflowKey());
  }

  @Test
  public void shouldCreateMultipleWorkflowInstancesWithRepeatingTimer() {
    // when
    testClient.deployWithResponse(THREE_SEC_MODEL);

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(1);
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(1);
    brokerRule.getClock().addTime(Duration.ofSeconds(3));

    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(1);
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(2);

    brokerRule.getClock().addTime(Duration.ofSeconds(3));
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(2);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(2);
  }

  @Test
  public void shouldCompleteWorkflow() {
    // when
    final ExecuteCommandResponse response = testClient.deployWithResponse(SIMPLE_MODEL);
    final DeploymentRecordValue deploymentRecord =
        testClient
            .receiveFirstDeploymentEvent(DeploymentIntent.CREATED, response.getKey())
            .getValue();

    // then
    Assertions.assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst())
        .isNotNull();
    brokerRule.getClock().addTime(Duration.ofSeconds(1));
    final WorkflowInstanceRecordValue instanceCompleted =
        RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
            .getFirst()
            .getValue();

    Assertions.assertThat(instanceCompleted).hasBpmnProcessId("process");
    Assertions.assertThat(instanceCompleted).hasVersion(1);
    Assertions.assertThat(instanceCompleted)
        .hasWorkflowKey(deploymentRecord.getDeployedWorkflows().get(0).getWorkflowKey());
  }

  @Test
  public void shouldUpdateWorkflow() {
    // when
    testClient.deploy(SIMPLE_MODEL);
    Assertions.assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).getFirst())
        .isNotNull();
    brokerRule.getClock().addTime(Duration.ofSeconds(1));

    // then
    Assertions.assertThat(
            RecordingExporter.workflowInstanceRecords(EVENT_ACTIVATED)
                .withElementId("end_1")
                .withBpmnProcessId("process")
                .withVersion(1)
                .getFirst())
        .isNotNull();

    // when
    testClient.deploy(REPEATING_MODEL);
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(2);
    brokerRule.getClock().addTime(Duration.ofSeconds(2));

    Assertions.assertThat(
            RecordingExporter.workflowInstanceRecords(EVENT_ACTIVATED)
                .withElementId("end_2")
                .withBpmnProcessId("process")
                .withVersion(2)
                .getFirst())
        .isNotNull();
  }

  @Test
  public void shouldReplaceTimerStartWithNoneStart() {
    // when
    testClient.deploy(REPEATING_MODEL);

    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(1);
    brokerRule.getClock().addTime(Duration.ofSeconds(1));

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);

    // when
    final BpmnModelInstance nonTimerModel =
        Bpmn.createExecutableProcess("process").startEvent("start_4").endEvent("end_4").done();
    testClient.deploy(nonTimerModel);

    assertThat(RecordingExporter.deploymentRecords(DeploymentIntent.CREATED).count()).isEqualTo(2);
    brokerRule.getClock().addTime(Duration.ofSeconds(2));

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.CANCELED).count()).isEqualTo(1);
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);

    final long workflowInstanceKey = testClient.createWorkflowInstance("process");

    final WorkflowInstanceRecordValue lastRecord =
        RecordingExporter.workflowInstanceRecords(EVENT_ACTIVATED)
            .withElementId("end_4")
            .getLast()
            .getValue();

    Assertions.assertThat(lastRecord).hasVersion(2);
    Assertions.assertThat(lastRecord).hasBpmnProcessId("process");
    Assertions.assertThat(lastRecord).hasWorkflowInstanceKey(workflowInstanceKey);
  }

  @Test
  public void shouldUpdateTimerPeriod() {
    // when
    long beginTime = brokerRule.getClock().getCurrentTimeInMillis();
    testClient.deploy(THREE_SEC_MODEL);
    assertThat(RecordingExporter.timerRecords(TimerIntent.CREATED).count()).isEqualTo(1);
    brokerRule.getClock().addTime(Duration.ofSeconds(3));

    // then
    TimerRecordValue timerRecord =
        RecordingExporter.timerRecords(TimerIntent.TRIGGERED).getFirst().getValue();

    Assertions.assertThat(timerRecord).hasDueDate(beginTime + 3000);

    // when
    beginTime = brokerRule.getClock().getCurrentTimeInMillis();
    final BpmnModelInstance slowerModel =
        Bpmn.createExecutableProcess("process_3")
            .startEvent("start_4")
            .timerWithCycle("R2/PT4S")
            .endEvent("end_4")
            .done();
    testClient.deploy(slowerModel);

    // then
    assertThat(RecordingExporter.timerRecords(TimerIntent.CANCELED).getFirst()).isNotNull();

    timerRecord = RecordingExporter.timerRecords(TimerIntent.CREATED).getLast().getValue();
    Assertions.assertThat(timerRecord).hasDueDate(beginTime + 4000);
    brokerRule.getClock().addTime(Duration.ofSeconds(3));
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(1);

    brokerRule.getClock().addTime(Duration.ofSeconds(1));
    assertThat(RecordingExporter.timerRecords(TimerIntent.TRIGGERED).count()).isEqualTo(2);
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

    brokerRule.getClock().addTime(Duration.ofSeconds(2));
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process")
                .count())
        .isEqualTo(2);
    assertThat(
            RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_READY)
                .withElementId("process_3")
                .count())
        .isEqualTo(1);
  }
}
