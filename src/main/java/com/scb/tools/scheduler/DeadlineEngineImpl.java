package com.scb.tools.scheduler;

import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class DeadlineEngineImpl implements DeadlineEngine {

    // 11 because this is the number in the default implementation
    // PriorityBlockingQueue works with concurrent access
    private final PriorityBlockingQueue<Deadline> deadlines = new PriorityBlockingQueue<>(11, Comparator.comparingLong(Deadline::getDeadlineMs));
    private final Set<Long> currentIds = ConcurrentHashMap.newKeySet();
    private final ReentrantLock lock = new ReentrantLock();

    @Override
    public long schedule(long deadlineMs) {
        long requestId;

        // Here we have to lock, as there is a race condition between:
        // - the time we check if an ID is in use
        // - the time we add it in the set
        lock.lock();
        try {
            requestId = getRandomLongNotInUse(currentIds);
            currentIds.add(requestId);
        } finally {
            lock.unlock();
        }

        deadlines.add(new Deadline(deadlineMs, requestId));

        return requestId;
    }

    @Override
    public boolean cancel(long requestId) {
        // A race condition should not happen here.
        // Example: a thread is creating a task, already added the ID to the currentIds set,
        // but has not yet added the deadline to the queue. Another thread try to delete this task,
        // but since it is not added in the queue (and queue methods add and removeIf cannot conflict)
        // the boolean "removed" will be false, and so it will not be removed from the currentIds set.

        // Another similar example, with the poll method: a thread is polling, so it removed the task from the queue
        // but kept the ID in currentIds. Since the task is polled, a thread cannot remove it and so it will not
        // be removed from the set of ID (since "removed" is false)

        // Of course, if a thread removes a deadline from the queue,
        // there will be some time before the ID is removed from the set.
        // But this should not cause any issue. What we do not want is to create a deadline with an ID
        // already in use.

        // Returns true if anything was removed
        boolean removed = deadlines.removeIf(deadline -> deadline.getRequestId() == requestId);

        if (removed)
            currentIds.remove(requestId);

        return removed;
    }

    @Override
    public int poll(long nowMs, Consumer<Long> handler, int maxPoll) {
        int i = 0;
        while (i < maxPoll) {
            // Poll it from the queue to make sure no other thread can take it
            Deadline deadline = deadlines.poll();

            // Cancel early if no more item
            if (deadline == null)
                break;

            // Or if deadline is later
            // since this is a priority queue, we know there is no deadline to check after this point.
            if (deadline.getDeadlineMs() > nowMs) {
                // We put it back and we exit the loop
                deadlines.put(deadline);
                break;
            }

            // Remove the ID from the currentIds set, and call the consumer.
            // Note: if the consumer throws, exception is logged and item is skipped.
            // This is a debatable design choice. In that case, we do not increment the counter.
            // Note 2: we have to lock, because by the time we remove the ID from the set, a thread can create
            // a new deadline. If we are unlucky, this other thread might reuse our ID,
            // which can be a source of issues.
            lock.lock();
            try {
                currentIds.remove(deadline.getRequestId());
                handler.accept(deadline.getRequestId());
                i++;
            } catch (Exception e) {
                System.err.println("Exception thrown in handler!");
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }

        return i;
    }

    @Override
    public int size() {
        return deadlines.size();
    }

    private static long getRandomLongNotInUse(Set<Long> alreadyInUse) {
        long id = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;

        // We keep generating until it does not collide anymore.
        // We are constrained to use a long here, but a real UUID would probably
        // be better to avoid this check.
        while (alreadyInUse.contains(id))
            id = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;

        return id;
    }

    private static class Deadline {

        private final long deadlineMs;
        private final long requestId;

        public Deadline(long deadlineMs, long requestId) {
            this.deadlineMs = deadlineMs;
            this.requestId = requestId;
        }

        public long getDeadlineMs() {
            return deadlineMs;
        }

        public long getRequestId() {
            return requestId;
        }

    }

}
