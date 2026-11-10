# Kubernetes GPU 集群上的 ML 训练与调度

本文讨论在 Kubernetes GPU 集群中运行机器学习训练任务，重点说明大模型如何使用数据并行、张量并行和流水线并行，以及 Airflow、训练框架和集群调度器各自负责什么。这里的 “pipeline parallelism” 指模型层的流水线并行，不是 Airflow DAG 的业务流水线。

## 1. 先区分三种“调度/任务”

训练平台里有几层不同的工作：

| 层 | 负责什么 | 示例 |
|---|---|---|
| 工作流编排 | 决定何时训练、依赖哪些数据、失败后如何重试、训练后是否评估/注册模型 | Airflow DAG |
| 集群队列与资源准入 | 决定训练作业何时获得一组 GPU 资源，避免资源不足时只启动一部分 worker | Kueue、Volcano，或云平台/集群提供的队列能力 |
| 分布式训练运行时 | 启动训练进程、分配 rank、划分数据/模型、执行通信和 checkpoint | PyTorch Distributed、DeepSpeed、Megatron-LM 等 |
| Kubernetes | 将获准运行的 Pod 放到有合适 GPU、网络和容量的节点上 | kube-scheduler、GPU device plugin |

Airflow 不负责把模型切片放进 GPU，也不应逐个调度每个训练 rank。通常 Airflow 的一个 task 提交**一个分布式训练作业**并等待其终态；作业控制器/训练运行时创建多个 worker，分布式运行时再在这些 worker 中启动训练进程。Kubernetes 负责 Pod 放置，队列/批调度器负责资源准入与成组启动。

```text
Airflow DAG
  → 准备/校验训练数据与配置
  → 提交一个 distributed training job
      → 队列准入并分配所需 GPU 容量
      → 创建 worker Pods
      → 训练运行时启动各个 rank 并执行分布式训练
  → 等待作业终态
  → 评估模型、通过质量门禁后注册/发布
```

不要把 Airflow task、Kubernetes Pod、训练 worker 和训练 rank 当作同一种“task”。一个 worker Pod 可以运行一个或多个训练进程；具体进程数、rank 布局由所选训练框架和 launcher 决定。

## 2. 大模型怎么“分”

### 2.1 数据并行（Data Parallelism，DP）

每个副本持有完整模型，不同副本处理不同的数据分片；每轮通过 collective communication（常见为 all-reduce）同步梯度或参数更新。增加 DP 副本通常提高吞吐，但不会让单个副本能容纳更大的模型。普通 DDP 属于这一类；ZeRO/FSDP 等方案还会切分或分片模型状态，因此内存行为与朴素 DDP 不同。

### 2.2 张量并行（Tensor Parallelism，TP）

把单层中的矩阵运算切到多个 GPU 上协作完成。它让单个模型层可以跨 GPU 运行，代价是层内频繁通信。通常优先把一个 TP group 放在高速互联紧密的 GPU 之间（例如同一节点的 NVLink 域）；是否能这样放置取决于模型、GPU 拓扑和框架支持。

### 2.3 流水线并行（Pipeline Parallelism，PP）

把模型的不同层分配给不同 stage。一个 micro-batch 依次经过各 stage；多个 micro-batch 可同时在流水线中推进。PP 能把模型层分布到多台机器，但会引入 stage 间传输、流水线空泡以及 stage 负载不均衡等成本。模型层如何切分一般由训练代码/框架定义，不是 Kubernetes scheduler 自动决定的。

### 2.4 混合并行及 GPU 数量

大模型训练经常组合 DP、TP、PP。简化的配置关系是：

```text
总训练进程数（world size）= DP × TP × PP
```

这假设每个并行 rank 使用一张 GPU；若使用 CPU 进程、专家并行或框架特定的 rank 编排，实际资源映射还要按框架配置核对。

例如，`DP=2, TP=4, PP=2` 时需要 16 个 GPU rank。若每台机器有 4 张 GPU，一个可能的布局是：每个 TP group 占满同一节点的 4 张 GPU；PP stage 分布在两台节点；第二个 DP replica 使用另一组两台节点。布局只是示意，必须根据模型分层、GPU 互联、节点网络和框架 rank mapping 验证。

**全局 batch size 与 TP/PP 不同。**常见同步训练设置下：

```text
global batch size = micro-batch size × DP × gradient accumulation steps
```

TP 和 PP 将同一批样本的模型计算拆分到多个 GPU，通常不直接乘进全局 batch 公式。实际定义可能受序列长度、损失归一化、框架实现和最后一个不完整 batch 影响，需以训练代码和框架文档为准。

因此，“把 batch 分给 GPU”通常说的是数据并行；“把模型层内计算切给多个 GPU”是张量并行；“把不同模型层放到不同 stage”是流水线并行。它们解决的问题和通信模式不同，不能互相替代。

## 3. Kubernetes 如何调度分布式 GPU 作业

### 3.1 先准入，再成组启动

分布式训练要求 worker 之间能够互相通信。假设作业需要 16 张 GPU，集群目前只能提供 12 张；若先启动部分 worker，它们可能一直等待其余 rank，白占 GPU。因此通常需要队列/批调度能力按作业整体做资源准入，并在满足策略时成组启动 worker（gang scheduling / PodGroup 一类机制）。

队列能力和训练作业控制器是不同职责：例如 Kueue 可用于作业排队和准入，Volcano 可用于批调度及 gang 语义；PyTorchJob 等训练控制器负责创建/管理训练 worker。具体产品组合、API 和功能取决于 Kubernetes 发行版和版本，采用前应验证版本兼容性及失败恢复行为。只有 GPU requests/limits 或 node affinity，不能单独保证多 Pod 同时获准和启动。

### 3.2 GPU requests、节点池和拓扑

- GPU 通常作为 Kubernetes extended resource 由 GPU device plugin 暴露；Pod 通过 GPU resource request 声明需要的卡数。具体资源名和切分/MIG 能力由硬件与集群配置决定。
- 使用 node labels、node affinity/selector 和 taints/tolerations，将工作负载限制到兼容的 GPU 型号、驱动/CUDA 栈或专用节点池。Affinity 只是选择约束，不会创建 GPU 容量。
- 对 TP 等通信密集的 group，需知道 GPU 到 GPU、GPU 到 NIC、NIC 到交换网络的拓扑；尽量把高频通信的 rank 放在互联更快的范围内。跨节点通信还依赖网络带宽、RDMA/GPUDirect 等集群能力，不能只看 GPU 数量。
- DP replicas 通常可跨节点或故障域分散；TP group 往往更依赖节点内高速互联。PP placement 要权衡 stage 间通信和节点资源。最终放置策略应由训练框架要求与实测共同决定。
- 配置每个 worker 的 CPU、内存、临时盘、本地缓存和网络需求。GPU 使用率低不一定意味着“GPU 不够”，也可能是数据读取、CPU preprocessing、通信或 batch 配置成为瓶颈。

资源申请应与训练并行配置一致。例如 16 个 GPU rank 不代表固定要 16 个 Pod：worker 数、每 Pod GPU 数、每 Pod 进程数，以及 rank 到 GPU 的映射由所用训练控制器和框架决定。平台需检查实际 Pod spec、rank 分布及 GPU 可见性，而不是只根据 YAML 中的副本数推断。

### 3.3 队列、配额和优先级

- 按团队/项目设置 GPU quota、队列容量、并发上限和成本标签；GPU 是稀缺资源，需有公平共享与优先级策略。
- 为交互式调试、短时验证和正式长训练定义不同队列或优先级；抢占前要确认作业能从 checkpoint 恢复。
- 防止单作业长期占满集群；同时避免过度碎片化造成大量 GPU 空闲但无法组成完整 worker group。
- 对需要固定 GPU 数量的训练作业，队列准入应考虑整个作业的资源需求和可用拓扑，不能只看集群总 GPU 卡数。
- 为排队时间、GPU 使用率、OOM、网络/通信错误和 checkpoint 成功率建立指标与告警。

## 4. 训练任务的提交、状态和恢复

Airflow DAG 通常负责编排业务生命周期，而不是细粒度训练步骤：

```text
检查数据/代码版本
  → 准备训练配置与资源需求
  → 提交分布式训练 CR/Job 到目标队列
  → 等待队列准入和训练作业终态
  → 检查 checkpoint 与训练指标
  → 执行离线评估
  → 通过门禁后注册模型版本
```

Airflow task 必须等待训练作业真实结束，不能把“提交 CR 成功”当成“训练成功”。如果 task 超时、被取消或 worker 重启，需能重新定位同一训练作业并继续监控；不可盲目再创建一组 GPU workers。重试策略应区分：

- **训练进程/worker 失败**：由训练控制器或框架按设定策略处理；部分 worker 失败是否全组重启取决于分布式运行时。
- **Airflow task 重试**：先确认既有训练作业状态，避免重复提交；若新建作业，应明确它是从 checkpoint 恢复还是从头训练。
- **抢占/节点故障**：使用定期 checkpoint，明确保存位置、恢复时需要的模型/优化器/随机状态及数据游标；写入持久且版本化的存储。

一次训练应记录代码 commit、容器镜像 digest、训练配置、并行参数（DP/TP/PP）、数据版本、队列/集群、GPU 型号与数量、checkpoint URI 和最终模型版本。容器镜像可包含训练代码及依赖，数据与 checkpoint 应放在受控的持久存储中。

## 5. 测试与发布：从代码到训练流水线

训练代码、Airflow DAG、训练镜像/依赖和环境配置是相关但不同的变更对象。PR 和发布可以只包含其中一部分；完整集成验收则通过 test Airflow 提交测试训练作业。

| 测试层 | 验证内容 | 通常是否用 GPU |
|---|---|---|
| 代码/配置单元测试 | 数据处理、模型逻辑、配置校验、并行维度及资源计算 | 否 |
| 单卡 smoke test | 模型能初始化、前向/反向、loss 与 checkpoint 基本正常 | 是，单卡或小型 GPU runner |
| 小规模分布式测试 | rank 初始化、通信、数据切分、DP/TP/PP 配置正确，worker 失败能按策略报告 | 是，少量 GPU |
| Airflow 集成测试 | test Airflow 是否提交正确训练配置、等待真实终态、传递失败/取消，并执行评估门禁 | 是，在 test GPU 集群 |
| Staging 验收 | 代表性数据下吞吐、显存、通信效率、恢复时间和成本 | 是，接近生产的资源形态 |

```text
PR：单元测试 + 配置/manifest 检查 + DAG 静态测试
  → 构建不可变训练镜像并记录 digest（若代码/依赖变化）
  → 部署候选 DAG/镜像/配置到 test（只部署本次变更对象）
  → test Airflow 触发小规模训练
  → 验证训练状态、checkpoint、评估指标和资源使用
  → 审批后晋级镜像、DAG 和配置中的相应版本
  → production Airflow 按获批 schedule/手动审批启动训练
```

单元测试不能代替分布式 GPU 测试：一个配置可能通过 Python 类型检查，却因 rank/world size 不一致、显存不足、GPU 拓扑不合适或 NCCL 网络配置错误而无法运行。反过来，集成测试通过也不能代替模型质量评估；模型准确性、收敛性、偏差/安全评估应有明确的数据集、指标和发布门槛。

## 6. 方案选择的基本问题

设计 GPU 训练平台前，先回答：

1. 集群使用 Kubernetes 还是 Slurm？Airflow 与 GPU 作业提交接口如何集成？
2. 训练控制器/运行时是什么？谁负责 worker 创建、rank 初始化、失败恢复和 checkpoint？
3. 队列如何做 GPU 配额、作业准入、优先级和 gang scheduling？
4. 目标模型与训练框架如何定义 DP/TP/PP、每 Pod GPU 数和 rank 布局？
5. GPU 节点之间有哪些互联拓扑与网络能力？有多少 GPU 可用于单个训练作业？
6. 数据、checkpoint、镜像和模型产物存放在哪里，分别如何授权和保留？
7. 训练是否支持抢占恢复？如何避免 Airflow 重试造成重复占卡或重复写模型？

先用小规模任务验证端到端链路，再根据模型尺寸、通信模式、GPU 利用率和排队数据扩展并行度。不要只根据“模型参数量”或“GPU 卡数”决定 DP/TP/PP；显存、序列长度、优化器状态、通信拓扑、吞吐目标和恢复要求都会影响设计。
