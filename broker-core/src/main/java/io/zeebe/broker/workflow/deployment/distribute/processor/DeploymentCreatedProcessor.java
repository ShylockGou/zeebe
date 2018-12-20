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
package io.zeebe.broker.workflow.deployment.distribute.processor;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.state.ZeebeState;
import io.zeebe.broker.subscription.command.SubscriptionCommandSender;
import io.zeebe.broker.system.configuration.ClusterCfg;
import io.zeebe.broker.workflow.data.TimerRecord;
import io.zeebe.broker.workflow.model.element.ExecutableCatchEventElement;
import io.zeebe.broker.workflow.processor.CatchEventBehavior;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.Workflow;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.transport.ClientTransport;
import org.agrona.DirectBuffer;

public class DeploymentCreatedProcessor implements TypedRecordProcessor<DeploymentRecord> {

  public static final int NO_ELEMENT_INSTANCE = -1;
  private ZeebeState zeebeState;
  private CatchEventBehavior catchEventBehavior;
  private ClusterCfg clusterConfig;

  public DeploymentCreatedProcessor(
      ZeebeState state, ClusterCfg clusterCfg, ClientTransport subscriptionClient) {
    this.zeebeState = state;
    this.clusterConfig = clusterCfg;
    this.catchEventBehavior =
        new CatchEventBehavior(
            state, new SubscriptionCommandSender(clusterCfg, subscriptionClient));
  }

  @Override
  public void processRecord(
      final TypedRecord<DeploymentRecord> event,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final DeploymentRecord deploymentEvent = event.getValue();

    if (inLowestPartitionId()) {
      createTimerIfTimerStartEvent(event, streamWriter);
    }

    streamWriter.appendFollowUpCommand(
        event.getKey(), DeploymentIntent.DISTRIBUTE, deploymentEvent);
  }

  private void createTimerIfTimerStartEvent(
      TypedRecord<DeploymentRecord> record, TypedStreamWriter streamWriter) {
    for (Workflow workflow : record.getValue().workflows()) {
      final WorkflowState workflowState = zeebeState.getWorkflowState();
      final ExecutableCatchEventElement startEvent =
          workflowState.getWorkflowByKey(workflow.getKey()).getWorkflow().getStartEvent();

      workflowState
          .getTimerState()
          .forEachTimerForElementInstance(
              NO_ELEMENT_INSTANCE,
              timer -> {
                final DirectBuffer timerBpmnId =
                    workflowState.getWorkflowByKey(timer.getWorkflowKey()).getBpmnProcessId();

                if (timerBpmnId.equals(workflow.getBpmnProcessId())) {
                  final TimerRecord timerRecord = new TimerRecord();
                  timerRecord.setElementInstanceKey(NO_ELEMENT_INSTANCE);
                  timerRecord.setDueDate(timer.getDueDate());
                  timerRecord.setRepetitions(timer.getRepetitions());
                  timerRecord.setWorkflowKey(workflow.getKey());
                  timerRecord.setHandlerNodeId(timer.getHandlerNodeId());

                  streamWriter.appendFollowUpCommand(
                      timer.getKey(), TimerIntent.CANCEL, timerRecord);
                }
              });

      if (startEvent.isTimer()) {
        catchEventBehavior.subscribeToTimerEvent(
            NO_ELEMENT_INSTANCE,
            workflow.getKey(),
            startEvent.getId(),
            startEvent.getTimer(),
            streamWriter);
      }
    }
  }

  private boolean inLowestPartitionId() {
    Integer minPartitionId = Integer.MAX_VALUE;
    for (Integer partitionId : clusterConfig.getPartitionIds()) {
      minPartitionId = Math.min(minPartitionId, partitionId);
    }

    return clusterConfig.getNodeId() == minPartitionId;
  }
}
