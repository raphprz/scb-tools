package com.scb.tools.scheduler;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class DeadlineEngineImplTest {

    // this map is used by the countCalls method to check that Consumer is called
    private final Map<Long, AtomicLong> callCounter = new ConcurrentHashMap<>();

    private void countCalls(Long id) {
        callCounter.putIfAbsent(id, new AtomicLong());
        callCounter.get(id).incrementAndGet();
    }

    private void countCallsButThrow(Long id) {
        callCounter.putIfAbsent(id, new AtomicLong());
        callCounter.get(id).incrementAndGet();

        throw new RuntimeException();
    }

    @Test
    public void testAddManyDeadlinesConcurrently()
            throws InterruptedException, ExecutionException {
        // create an executor to call the scheduler many times in parallel
        ExecutorService executor = Executors.newFixedThreadPool(24);

        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        int numberOfDeadlines = 10000;
        int randomNumberUpperBound = 5;
        long now = Instant.now().toEpochMilli();
        List<Callable<Long>> tasks = new ArrayList<>();

        for (int i = 0; i < numberOfDeadlines; i++) {
            // This will produce many scheduled task around the current time,
            // some before, some after, some equal to now
            int randomNum = ThreadLocalRandom.current().nextInt(-randomNumberUpperBound, randomNumberUpperBound);

            tasks.add(() -> scheduler.schedule(now + randomNum));
        }

        List<Future<Long>> futures = executor.invokeAll(tasks);

        List<Long> results = new ArrayList<>();
        for (Future<Long> future : Objects.requireNonNull(futures)) {
            Long s = future.get();
            results.add(s);
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        assertThat(results).hasSize(numberOfDeadlines);
        assertThat(scheduler.size()).isEqualTo(numberOfDeadlines);
    }

    @Test
    public void testCancelSomeDeadlines() {
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        long id1 = scheduler.schedule(now);
        long id2 = scheduler.schedule(now - 1);
        long id3 = scheduler.schedule(now + 1);

        assertThat(scheduler.size()).isEqualTo(3);

        boolean cancelled1 = scheduler.cancel(id1);
        assertThat(cancelled1).isTrue();
        assertThat(scheduler.size()).isEqualTo(2);

        boolean cancelled2 = scheduler.cancel(id2);
        assertThat(cancelled2).isTrue();
        assertThat(scheduler.size()).isEqualTo(1);

        boolean cancelled3 = scheduler.cancel(id3);
        assertThat(cancelled3).isTrue();
        assertThat(scheduler.size()).isEqualTo(0);
    }

    @Test
    public void testCancelNotExistingDeadline() {
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        long id1 = scheduler.schedule(now);
        long id2 = scheduler.schedule(now - 1);
        long id3 = scheduler.schedule(now + 1);

        Set<Long> currentIds = Set.of(id1, id2, id3);
        long randomIdDifferentThanOthers = ThreadLocalRandom.current().nextLong();

        while (currentIds.contains(randomIdDifferentThanOthers))
            randomIdDifferentThanOthers = ThreadLocalRandom.current().nextLong();

        assertThat(scheduler.size()).isEqualTo(3);

        boolean cancelled = scheduler.cancel(randomIdDifferentThanOthers);
        assertThat(cancelled).isFalse();
        assertThat(scheduler.size()).isEqualTo(3);
    }

    @Test
    public void testPollAllEligibleDeadlines() {
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        scheduler.schedule(now);
        scheduler.schedule(now + 1);
        scheduler.schedule(now - 1);

        assertThat(scheduler.size()).isEqualTo(3);

        // normally, deadlines 1 and 2 should be polled
        int polled = scheduler.poll(now, System.out::println, 3);
        assertThat(polled).isEqualTo(2);
        assertThat(scheduler.size()).isEqualTo(1);
    }

    @Test
    public void testPollDeadlinesMaxPollZero_ShouldNotPollAnything() {
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        scheduler.schedule(now);
        scheduler.schedule(now + 1);
        scheduler.schedule(now - 1);

        assertThat(scheduler.size()).isEqualTo(3);

        // normally, deadlines 1 and 2 should be polled
        int polled = scheduler.poll(now, System.out::println, 0);
        assertThat(polled).isEqualTo(0);
        assertThat(scheduler.size()).isEqualTo(3);
    }

    @Test
    public void testPollDeadlinesMaxPollOne_ShouldOnlyPollOne() {
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        scheduler.schedule(now);
        scheduler.schedule(now + 1);
        scheduler.schedule(now - 1);

        assertThat(scheduler.size()).isEqualTo(3);

        // normally, deadlines 1 and 2 should be polled
        int polled = scheduler.poll(now, System.out::println, 1);
        assertThat(polled).isEqualTo(1);
        assertThat(scheduler.size()).isEqualTo(2);
    }

    @Test
    public void testPollAllEligibleDeadlines_ShouldIncreaseCounter() {
        // This test is there to verify the Consumer is properly called
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        scheduler.schedule(now);
        scheduler.schedule(now + 1);
        scheduler.schedule(now - 1);

        assertThat(scheduler.size()).isEqualTo(3);

        // normally, deadlines 1 and 2 should be polled
        int polled = scheduler.poll(now, this::countCalls, 3);
        assertThat(polled).isEqualTo(2);
        assertThat(scheduler.size()).isEqualTo(1);

        assertThat(callCounter).hasSize(2);
        assertThat(callCounter.values().stream()
                .map(AtomicLong::get)
                .collect(Collectors.toList())).containsExactly(1L, 1L);
    }

    @Test
    public void testPollMaxPollGreaterThanSchedulerSize_ShouldNotCauseIssue() {
        // This test is there to verify the Consumer is properly called
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        scheduler.schedule(now);
        scheduler.schedule(now - 1);
        scheduler.schedule(now - 2);

        assertThat(scheduler.size()).isEqualTo(3);

        // normally, deadlines 1 and 2 should be polled
        int polled = scheduler.poll(now, this::countCalls, 4);
        assertThat(polled).isEqualTo(3);
        assertThat(scheduler.size()).isEqualTo(0);

        assertThat(callCounter).hasSize(3);
        assertThat(callCounter.values().stream()
                .map(AtomicLong::get)
                .collect(Collectors.toList())).containsExactly(1L, 1L, 1L);
    }

    @Test
    public void testPollWithThrowingConsumer() {
        // This test is there to verify the Consumer is ignored when throwing,
        // but also that the count returned by poll stays at zero
        DeadlineEngineImpl scheduler = new DeadlineEngineImpl();

        long now = Instant.now().toEpochMilli();

        scheduler.schedule(now);

        assertThat(scheduler.size()).isEqualTo(1);

        // normally, deadlines 1 and 2 should be polled
        int polled = scheduler.poll(now, this::countCallsButThrow, 1);
        assertThat(polled).isEqualTo(0);
        assertThat(scheduler.size()).isEqualTo(0);

        assertThat(callCounter).hasSize(1);
        assertThat(callCounter.values().stream()
                .map(AtomicLong::get)
                .collect(Collectors.toList())).containsExactly(1L);
    }

}