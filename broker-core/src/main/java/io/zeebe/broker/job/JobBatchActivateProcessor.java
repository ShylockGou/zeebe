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
package io.zeebe.broker.job;

import static io.zeebe.util.sched.clock.ActorClock.currentTimeMillis;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.msgpack.value.LongValue;
import io.zeebe.msgpack.value.ValueArray;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.record.value.job.JobBatchRecord;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.intent.JobBatchIntent;
import io.zeebe.protocol.intent.JobIntent;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.ExpandableArrayBuffer;

public class JobBatchActivateProcessor implements TypedRecordProcessor<JobBatchRecord> {

  private final JobState state;

  public JobBatchActivateProcessor(final JobState state) {
    this.state = state;
  }

  @Override
  public void processRecord(
      final TypedRecord<JobBatchRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final JobBatchRecord value = record.getValue();
    if (isValid(value)) {
      activateJobs(record, responseWriter, streamWriter);
    } else {
      rejectCommand(record, responseWriter, streamWriter);
    }
  }

  private boolean isValid(final JobBatchRecord record) {
    return record.getAmount() > 0
        && record.getTimeout() > 0
        && record.getType().capacity() > 0
        && record.getWorker().capacity() > 0;
  }

  private void activateJobs(
      final TypedRecord<JobBatchRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final JobBatchRecord value = record.getValue();

    final long jobBatchKey = streamWriter.getKeyGenerator().nextKey();

    final AtomicInteger amount = new AtomicInteger(value.getAmount());
    collectJobsToActivate(value, amount);

    // Collecting of jobs and update state and write ACTIVATED job events should be separate,
    // since otherwise this will cause some problems (weird behavior) with the reusing of objects
    //
    // ArrayProperty.add will give always the same object - state.activate will
    // set/use this object for writing the new job state
    activateJobs(streamWriter, value);

    streamWriter.appendFollowUpEvent(jobBatchKey, JobBatchIntent.ACTIVATED, value);
    responseWriter.writeEventOnCommand(jobBatchKey, JobBatchIntent.ACTIVATED, value, record);
  }

  private void collectJobsToActivate(JobBatchRecord value, AtomicInteger amount) {
    final ValueArray<JobRecord> jobIterator = value.jobs();
    final ValueArray<LongValue> jobKeyIterator = value.jobKeys();

    // collect jobs for activation
    state.forEachActivatableJobs(
        value.getType(),
        (key, jobRecord) -> {
          final int remainingAmount = amount.decrementAndGet();
          if (remainingAmount >= 0) {
            final long deadline = currentTimeMillis() + value.getTimeout();
            jobKeyIterator.add().setValue(key);
            final JobRecord arrayValueJob = jobIterator.add();

            // clone job record to modify and add it
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(jobRecord.getLength());
            jobRecord.write(buffer, 0);

            arrayValueJob.wrap(buffer);
            arrayValueJob.setDeadline(deadline).setWorker(value.getWorker());
          }

          return remainingAmount > 0;
        });
  }

  private void activateJobs(TypedStreamWriter streamWriter, JobBatchRecord value) {
    final Iterator<JobRecord> iterator = value.jobs().iterator();
    final Iterator<LongValue> keyIt = value.jobKeys().iterator();
    while (iterator.hasNext() && keyIt.hasNext()) {
      final JobRecord next = iterator.next();
      final LongValue next1 = keyIt.next();
      final long key = next1.getValue();

      final ExpandableArrayBuffer copy = new ExpandableArrayBuffer();
      next.write(copy, 0);
      final JobRecord jobRecord = new JobRecord();
      jobRecord.wrap(copy, 0, next.getLength());

      // update state and write follow up event for job record
      state.activate(key, jobRecord);
      streamWriter.appendFollowUpEvent(key, JobIntent.ACTIVATED, next);
    }
  }

  private void rejectCommand(
      final TypedRecord<JobBatchRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final RejectionType rejectionType;
    final String rejectionReason;

    final JobBatchRecord value = record.getValue();

    if (value.getAmount() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch amount must be greater than zero, got " + value.getAmount();
    } else if (value.getTimeout() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch timeout must be greater than zero, got " + value.getTimeout();
    } else if (value.getType().capacity() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch type must not be empty";
    } else if (value.getWorker().capacity() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch worker must not be empty";
    } else {
      throw new IllegalStateException("Job batch command is valid and should not be rejected");
    }

    streamWriter.appendRejection(record, rejectionType, rejectionReason);
    responseWriter.writeRejectionOnCommand(record, rejectionType, rejectionReason);
  }
}
