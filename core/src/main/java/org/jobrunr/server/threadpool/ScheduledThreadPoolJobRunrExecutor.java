package org.jobrunr.server.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;

public class ScheduledThreadPoolJobRunrExecutor extends java.util.concurrent.ScheduledThreadPoolExecutor implements JobRunrExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScheduledThreadPoolJobRunrExecutor.class);

    private final AntiDriftScheduler antiDriftScheduler;

    public ScheduledThreadPoolJobRunrExecutor(int corePoolSize, String threadNamePrefix) {
        super(corePoolSize + 1, new NamedThreadFactory(threadNamePrefix));
        setMaximumPoolSize(corePoolSize * 2 + 2);
        setKeepAliveTime(1, TimeUnit.MINUTES);
        antiDriftScheduler = new AntiDriftScheduler(this);
        super.scheduleAtFixedRate(antiDriftScheduler, 0, 250, TimeUnit.MILLISECONDS);
    }

    public void scheduleAtFixedRate(Runnable command, Duration initialDelay, Duration period) {
        this.antiDriftScheduler.addSchedule(new AntiDriftSchedule(command, initialDelay, period));
    }

    public ScheduledFuture<?> schedule(Runnable command, Duration delay) {
        return this.schedule(command, delay.toMillis(), TimeUnit.MILLISECONDS);
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

        public AntiDriftScheduler(ScheduledThreadPoolJobRunrExecutor executor) {
            this.executor = executor;
            this.antiDriftSchedules = new CopyOnWriteArrayList<>();
        }

        public void addSchedule(AntiDriftSchedule antiDriftSchedule) {
            this.antiDriftSchedules.add(antiDriftSchedule);
        }

        @Override
        public void run() {
            Instant now = Instant.now();
            antiDriftSchedules.stream()
                    .filter(antiDriftSchedule -> antiDriftSchedule.getScheduledAt().isBefore(now))
                    .forEach(this::schedule);
        }

        private void schedule(AntiDriftSchedule antiDriftSchedule) {
            Instant now = Instant.now();
            Instant nextSchedule = antiDriftSchedule.getNextSchedule();
            Duration duration = Duration.between(now, nextSchedule);
            executor.schedule(antiDriftSchedule.runnable, duration);
        }
    }

    private static class AntiDriftSchedule {
        private final Duration initialDelay;
        private final Duration duration;
        private final Runnable runnable;
        private final Instant firstScheduledAt;
        private long scheduleCount;
        private Instant scheduledAt;

        public AntiDriftSchedule(Runnable runnable, Duration initialDelay, Duration duration) {
            this.runnable = runnable;
            this.initialDelay = initialDelay;
            this.duration = duration;
            this.scheduleCount = 0;
            this.firstScheduledAt = Instant.now().plus(initialDelay);
            this.scheduledAt = firstScheduledAt;
        }

        public Instant getScheduledAt() {
            return this.scheduledAt;
        }

        public Instant getNextSchedule() {
            this.scheduledAt = firstScheduledAt.plus(duration.multipliedBy(scheduleCount));
            scheduleCount++;
            return scheduledAt;
        }
    }
}
