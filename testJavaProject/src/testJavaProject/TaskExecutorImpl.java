package testJavaProject;

import java.util.UUID;
import java.util.concurrent.*;

public class TaskExecutorImpl implements TaskExecutor {

    private final ExecutorService executorService;
    private final BlockingQueue<Task<?>> taskQueue;
    private final ConcurrentHashMap<UUID, Future<?>> runningGroups;
    private volatile boolean isRunning; 

    public TaskExecutorImpl(int maxConcurrency) {
        System.out.println("Initializing TaskExecutorImpl");
        this.executorService = Executors.newFixedThreadPool(maxConcurrency);
        this.taskQueue = new LinkedBlockingQueue<>();
        this.runningGroups = new ConcurrentHashMap<>();
        this.isRunning = true; 
        startTaskProcessing();
    }

    private void startTaskProcessing() {
        executorService.submit(() -> {
            while (isRunning) {
                try {
                    Task<?> task = taskQueue.take(); 
                    System.out.println("Task picked up for processing: " + task.taskGroup() + ", " + task.taskType());

                    UUID groupUUID = task.taskGroup().groupUUID();

                    while (runningGroups.containsKey(groupUUID)) {
                        System.out.println("Waiting for tasks in the same group to finish");
                        Thread.sleep(300); 
                    }

                    Future<?> future = executorService.submit(() -> {
                        try {
                            System.out.println("Executing task");
                            return task.taskAction().call();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            runningGroups.remove(groupUUID);
                        }
                    });

                    System.out.println("Task added to running groups");
                    runningGroups.put(groupUUID, future);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    if (!isRunning) {
                        break; 
                    }
                }
            }
        });
    }

    @Override
    public <T> Future<T> submitTask(Task<T> task) {
        CompletableFuture<T> future = new CompletableFuture<>();
        taskQueue.offer(new Task<>(task.taskUUID(), task.taskGroup(), task.taskType(), () -> {
            T result = task.taskAction().call();
            future.complete(result);
            return result;
        }));
        return future;
    }

    public void shutdown() {
        isRunning = false; 
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }


    public static void main(String[] args) throws ExecutionException, InterruptedException {
        TaskExecutorImpl executor = new TaskExecutorImpl(5);

        TaskGroup group1 = new TaskGroup(UUID.randomUUID());
        TaskGroup group2 = new TaskGroup(UUID.randomUUID());

        Task<String> task1 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
            Thread.sleep(3000);
            return "Task 1 completed";
        });

        Task<String> task2 = new Task<>(UUID.randomUUID(), group1, TaskType.WRITE, () -> {
            return "Task 2 completed";
        });

        Task<String> task3 = new Task<>(UUID.randomUUID(), group2, TaskType.READ, () -> {
            return "Task 3 completed";
        });

        Future<String> future1 = executor.submitTask(task1);
        Future<String> future2 = executor.submitTask(task2);
        Future<String> future3 = executor.submitTask(task3);

        System.out.println(future1.get()); 
        System.out.println(future2.get());
        System.out.println(future3.get());

        executor.shutdown(); 
    }
}

// Interface and classes
interface TaskExecutor {
    <T> Future<T> submitTask(Task<T> task);
}

enum TaskType {
    READ,
    WRITE
}

record Task<T>(UUID taskUUID, TaskGroup taskGroup, TaskType taskType, Callable<T> taskAction) {
    public Task {
        if (taskUUID == null || taskGroup == null || taskType == null || taskAction == null) {
            throw new IllegalArgumentException("All parameters must not be null");
        }
    }
}

record TaskGroup(UUID groupUUID) {
    public TaskGroup {
        if (groupUUID == null) {
            throw new IllegalArgumentException("All parameters must not be null");
        }
    }
}
