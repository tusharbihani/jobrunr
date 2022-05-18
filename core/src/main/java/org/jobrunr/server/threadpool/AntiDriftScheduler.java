package org.jobrunr.server.threadpool;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

class AntiDriftScheduler implements Runnable {

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
        Instant nextSchedule = antiDriftSchedule.getNextSchedule();
        Duration duration = Duration.between(Instant.now(), nextSchedule);
        executor.schedule(antiDriftSchedule.getRunnable(), duration);
    }
}
