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
package io.zeebe.broker.workflow.processor.flownode;

import io.zeebe.broker.workflow.model.element.ExecutableFlowNode;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.protocol.impl.record.value.incident.ErrorType;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class CompleteFlowNodeHandler implements BpmnStepHandler<ExecutableFlowNode> {
  private final IOMappingHelper ioMappingHelper = new IOMappingHelper();
  private final WorkflowState state;

  public CompleteFlowNodeHandler(WorkflowState state) {
    this.state = state;
  }

  @Override
  public void handle(BpmnStepContext<ExecutableFlowNode> context) {
    try {
      ioMappingHelper.applyOutputMappings(state, context);

      complete(context);

      context
          .getOutput()
          .appendFollowUpEvent(
              context.getRecord().getKey(),
              WorkflowInstanceIntent.ELEMENT_COMPLETED,
              context.getValue());
    } catch (MappingException e) {
      context.raiseIncident(ErrorType.IO_MAPPING_ERROR, e.getMessage());
    }
  }

  /**
   * To be overridden by subclasses
   *
   * @param context current processor context
   */
  public void complete(BpmnStepContext<ExecutableFlowNode> context) {}
}
