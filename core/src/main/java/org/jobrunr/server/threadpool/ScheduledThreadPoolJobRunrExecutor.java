package org.jobrunr.server.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ScheduledThreadPoolJobRunrExecutor extends java.util.concurrent.ScheduledThreadPoolExecutor implements JobRunrExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledThreadPoolJobRunrExecutor.class);

    private AntiDriftScheduler antiDriftScheduler;

    public ScheduledThreadPoolJobRunrExecutor(int corePoolSize, String threadNamePrefix) {
        super(corePoolSize + 1, new NamedThreadFactory(threadNamePrefix));
        setMaximumPoolSize(corePoolSize * 2);
        setKeepAliveTime(1, TimeUnit.MINUTES);
        antiDriftScheduler = new AntiDriftScheduler(this, Arrays.asList(new AntiDriftSchedule(Duration.ofMinutes(1), new TestClass())));
        scheduleAtFixedRate(antiDriftScheduler, 0, 250, TimeUnit.MILLISECONDS);
    }

    @Override
    public int getPriority() {
        return 10;
    }

    @Override
    public void start() {
        this.prestartAllCoreThreads();
        LOGGER.info("ThreadManager of type 'ScheduledThreadPool' started");
    }

    @Override
    public void stop() {
        shutdown();
        try {
            if (!awaitTermination(10, TimeUnit.SECONDS)) {
                shutdownNow();
            }
        } catch (InterruptedException e) {
            shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {

        private final String poolName;
        private final ThreadFactory threadFactory;

        public NamedThreadFactory(String poolName) {
            this.poolName = poolName;
            threadFactory = Executors.defaultThreadFactory();
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = threadFactory.newThread(runnable);
            thread.setName(thread.getName().replace("pool", poolName));
            return thread;
        }
    }

    private static class AntiDriftScheduler implements Runnable {

        private final ScheduledThreadPoolJobRunrExecutor executor;
        private final List<AntiDriftSchedule> antiDriftSchedules;

        public AntiDriftScheduler(ScheduledThreadPoolJobRunrExecutor executor, List<AntiDriftSchedule> antiDriftSchedules) {
            this.executor = executor;
            this.antiDriftSchedules = antiDriftSchedules;
        }

        @Override
        public void run() {
            Instant now = Instant.now();
            antiDriftSchedules.stream()
                    .filter(antiDriftSchedule -> antiDriftSchedule.scheduledAt.isBefore(now))
                    .forEach(this::schedule);
        }

        private void schedule(AntiDriftSchedule antiDriftSchedule) {
            Instant now = Instant.now();
            Instant nextSchedule = antiDriftSchedule.getNextSchedule();
            Duration duration = Duration.between(now, nextSchedule);
            executor.schedule(antiDriftSchedule.runnable, duration.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static class AntiDriftSchedule {
        private final Duration duration;
        private final Runnable runnable;
        private final Instant firstScheduledAt;
        private long scheduleCount;
        private Instant scheduledAt;

        public AntiDriftSchedule(Duration duration, Runnable runnable) {
            this.duration = duration;
            this.runnable = runnable;
            this.firstScheduledAt = Instant.now();
            this.scheduleCount = 0;
            this.scheduledAt = Instant.now();
        }

        public Instant getNextSchedule() {
            this.scheduledAt = firstScheduledAt.plus(duration.multipliedBy(scheduleCount));
            scheduleCount++;
            return scheduledAt;
        }

    }

    private static class TestClass implements Runnable {

        private Instant firstRun;
        private long  runCounter;

        @Override
        public void run() {
            if(firstRun == null) firstRun = Instant.now();
            Instant expectedRunInstant = firstRun.plus(Duration.ofMinutes(1).multipliedBy(runCounter));
            LOGGER.info("AntiDriftScheduler started at {}; now is {}; drift: {}", firstRun, Instant.now(), Duration.between(Instant.now(), expectedRunInstant).toMillis());
            runCounter++;
        }
    }
}
