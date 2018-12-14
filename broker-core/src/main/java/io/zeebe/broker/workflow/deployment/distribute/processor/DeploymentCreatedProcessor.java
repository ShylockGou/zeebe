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
import io.zeebe.broker.workflow.data.TimerRecord;
import io.zeebe.broker.workflow.model.element.ExecutableCatchEventElement;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.Workflow;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.TimerIntent;
import io.zeebe.util.sched.clock.ActorClock;
import org.agrona.DirectBuffer;

public class DeploymentCreatedProcessor implements TypedRecordProcessor<DeploymentRecord> {

  private WorkflowState workflowState;

  public DeploymentCreatedProcessor(WorkflowState workflowState) {
    this.workflowState = workflowState;
  }

  @Override
  public void processRecord(
      final TypedRecord<DeploymentRecord> event,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final DeploymentRecord deploymentEvent = event.getValue();
    createTimerIfTimerStartEvent(event, streamWriter);
    streamWriter.appendFollowUpCommand(
        event.getKey(), DeploymentIntent.DISTRIBUTE, deploymentEvent);
  }

  private void createTimerIfTimerStartEvent(
      TypedRecord<DeploymentRecord> record, TypedStreamWriter streamWriter) {
    for (Workflow workflow : record.getValue().workflows()) {
      final ExecutableCatchEventElement startEvent =
          workflowState.getWorkflowByKey(workflow.getKey()).getWorkflow().getStartEvent();

      final DirectBuffer workflowBpmnId = workflow.getBpmnProcessId();
      workflowState
          .getTimerState()
          .forEachTimerForElementInstance(
              -1,
              timer -> {
                if (timer.getBpmnId().equals(workflowBpmnId)) {
                  workflowState.getTimerState().remove(timer);
                }
              });

      if (startEvent.isTimer()) {
        final TimerRecord timerRecord = new TimerRecord();

        timerRecord.setElementInstanceKey(-1);
        timerRecord.setRepetitions(startEvent.getTimer().getRepetitions());
        final long nowMs = ActorClock.currentTimeMillis();
        final long dueDate = startEvent.getTimer().getInterval().toEpochMilli(nowMs);
        timerRecord.setDueDate(dueDate);
        timerRecord.setHandlerNodeId(startEvent.getId());
        timerRecord.setBpmnId(workflow.getBpmnProcessId());

        streamWriter.appendNewCommand(TimerIntent.CREATE, timerRecord);
      }
    }
  }
}
