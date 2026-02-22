# Progress Callback Refactoring

## 概述

将 `sendProgressCallback` 方法从繁杂的参数列表重构为使用结构化参数对象，提高代码可读性和可维护性。

## 改动内容

### 1. Java 端

**文件**: `common-java/src/main/java/com/graphbench/api/ProgressCallback.java`

#### 新增 ProgressEvent 类

```java
public static class ProgressEvent {
    public String event;
    public String taskName;
    public String workloadFile;
    public String status;
    public Double durationSeconds;
    public int taskIndex;
    public int totalTasks;
    public Integer originalOpsCount;
    public Integer validOpsCount;
    public Integer filteredOpsCount;
    public Integer numOps;

    // 构造函数和流式API方法
    public ProgressEvent(String event, String taskName) { ... }
    public ProgressEvent workloadFile(String workloadFile) { ... }
    public ProgressEvent status(String status) { ... }
    public ProgressEvent duration(Double durationSeconds) { ... }
    public ProgressEvent taskProgress(int taskIndex, int totalTasks) { ... }
    public ProgressEvent opsCounts(Integer original, Integer valid, Integer filtered) { ... }
    public ProgressEvent numOps(Integer numOps) { ... }
}
```

#### 使用示例

**之前**:
```java
progressCallback.sendProgressCallback(
    "task_start", taskType, workloadFile.getName(),
    null, null, taskIndex, totalTasks
);
```

**之后**:
```java
progressCallback.sendProgressCallback(
    new ProgressCallback.ProgressEvent("task_start", taskType)
        .workloadFile(workloadFile.getName())
        .taskProgress(taskIndex, totalTasks)
);
```

### 2. C++ 端

**文件**: `common-cpp/include/graphbench/progress_callback.hpp`

#### 新增 ProgressEvent 结构体

```cpp
struct ProgressEvent {
    std::string event;
    std::string taskName;
    std::optional<std::string> workloadFile;
    std::optional<std::string> status;
    std::optional<double> durationSeconds;
    int taskIndex = 0;
    int totalTasks = 0;
    std::optional<int> originalOpsCount;
    std::optional<int> validOpsCount;
    std::optional<int> filteredOpsCount;
    std::optional<int> numOps;

    // 构造函数和流式API方法
    ProgressEvent(const std::string& event, const std::string& taskName);
    ProgressEvent& setWorkloadFile(const std::string& file);
    ProgressEvent& setStatus(const std::string& s);
    ProgressEvent& setDuration(double duration);
    ProgressEvent& setTaskProgress(int index, int total);
    ProgressEvent& setOpsCounts(int original, int valid, int filtered);
    ProgressEvent& setNumOps(int ops);
};
```

#### 使用示例

**之前**:
```cpp
progressCallback_->sendProgressCallback(
    "task_start", taskType, workloadFile.filename().string(),
    "", -1.0, taskIndex, totalTasks
);
```

**之后**:
```cpp
progressCallback_->sendProgressCallback(
    ProgressEvent("task_start", taskType)
        .setWorkloadFile(workloadFile.filename().string())
        .setTaskProgress(taskIndex, totalTasks)
);
```

## 优势

1. **参数明确**: 每个参数都有明确的名称，不会混淆
2. **可读性强**: 调用代码更清晰，一眼就能看出设置了哪些参数
3. **易于扩展**: 添加新参数不需要修改所有调用点
4. **类型安全**:
   - Java 使用包装类型（Integer, Double）表示可选参数
   - C++ 使用 `std::optional` 表示可选参数，避免魔法值（如 -1）
5. **流式API**: 支持链式调用，代码更简洁

## 影响范围

### 修改的文件

1. `common-java/src/main/java/com/graphbench/api/ProgressCallback.java`
2. `common-java/src/main/java/com/graphbench/api/WorkloadDispatcher.java`
3. `common-cpp/include/graphbench/progress_callback.hpp`
4. `common-cpp/include/graphbench/workload_dispatcher.hpp`

### 验证

所有数据库的 Docker 镜像构建成功：
- ✅ Neo4j (Java)
- ✅ Aster (C++)
- ✅ ArangoDB (C++)

## 向后兼容性

此重构是破坏性更改，旧的参数列表方法已被移除。所有调用点都已更新。

## 示例对比

### 完整示例 - 带操作计数的子任务完成回调

**Java - 之前**:
```java
progressCallback.sendProgressCallback(
    "subtask_complete", subtaskName, null, "success", taskDuration,
    taskIndex, totalTasks, originalOpsCount, validOpsCount, filteredOpsCount
);
```

**Java - 之后**:
```java
progressCallback.sendProgressCallback(
    new ProgressCallback.ProgressEvent("subtask_complete", subtaskName)
        .status("success")
        .duration(taskDuration)
        .taskProgress(taskIndex, totalTasks)
        .opsCounts(originalOpsCount, validOpsCount, filteredOpsCount)
);
```

**C++ - 之前**:
```cpp
progressCallback_->sendProgressCallback(
    "subtask_complete", subtaskName, "",
    "success", duration, taskIndex, totalTasks,
    originalCount, validCount, filteredCount
);
```

**C++ - 之后**:
```cpp
progressCallback_->sendProgressCallback(
    ProgressEvent("subtask_complete", subtaskName)
        .setStatus("success")
        .setDuration(duration)
        .setTaskProgress(taskIndex, totalTasks)
        .setOpsCounts(originalCount, validCount, filteredCount)
);
```
