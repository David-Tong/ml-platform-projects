# Spark on Kubernetes：测试、生产环境与作业运维

本文补充说明如何在 Kubernetes 上管理 Spark 的测试和生产环境、保持环境一致、分配资源、监控作业并编排工作流。先给出核心建议：

1. **每个长期环境使用独立 Kubernetes cluster**：dev、test、staging 和 production 分别部署到各自集群，不用 namespace 代替环境隔离。
2. **集群内按业务划分 Spark namespace，并按实际容量需要配置 node pool**。Namespace 是业务的权限和配额边界，不是独立资源池；只有需要限定 Spark 可用节点时才用 node affinity 选择节点池，Pod affinity/anti-affinity 则按明确的共置/分散目标配置。
3. **业务团队晋级的是同一版本的 Spark 作业代码/制品**；Spark 运行时镜像由平台提供并维护，业务侧固定并记录所使用的镜像版本。
4. **Airflow 等工作流编排器管理业务依赖和日历，并通过 `spark-submit` 将 Spark Application 提交到集群**。本方案不使用 Spark Operator；不要让多个系统同时负责同一层重试和状态。

文中的 namespace、资源量和配置均为示例。实际值需根据集群容量、Spark 版本、作业并行度及负载测试确定。

## 1. 测试与生产环境怎么管理

### 1.1 环境层次

| 环境 | 用途 | 隔离建议 |
|---|---|---|
| 本地/CI | 单元测试、代码检查、快速小数据测试 | 不连接生产数据；用合成数据和临时凭证 |
| 集成测试 | 验证 Spark 镜像、Kubernetes 提交、S3 权限和 connector | 独立 test cluster 中的 Spark namespace、专用 bucket/prefix 和 IAM role；小规模资源上限 |
| Staging/预发 | 用接近生产的配置跑代表性数据和发布候选版本 | 独立 staging cluster；匹配生产的 Spark 配置、节点规格和调度约束；数据需脱敏或获批 |
| Production/生产 | 按 SLA 执行正式数据流水线 | 独立生产集群、访问控制、身份、数据路径、监控告警和发布审批 |

本方案以**每个长期环境使用独立 Kubernetes cluster**为基线：dev、test、staging 和 production 各自拥有集群，避免把环境边界寄托在 namespace 上。集群隔离有助于隔离节点故障、管理员权限、控制平面容量、升级窗口和资源争用。每个集群内再按业务划分 Spark namespace，例如票价预测与活动推荐分别使用独立 namespace；这样可以分别配置 RBAC、ResourceQuota、ServiceAccount 和审计规则。若成本或平台条件迫使环境共用 cluster，应视为有明确风险接受的例外。

### 1.2 如何按业务划分 Namespace

环境隔离由 cluster 提供，不在同一 cluster 里创建 `dev`、`test`、`staging` 等环境 namespace。Namespace 按业务划分，同一业务在各环境 cluster 使用对应的 namespace；通常不需要每个 Spark job 建一个 namespace：

```text
dev cluster:
  spark-ticket-pricing
  spark-event-recommendation

test cluster:
  spark-ticket-pricing
  spark-event-recommendation

staging cluster:
  spark-ticket-pricing
  spark-event-recommendation

production cluster:
  spark-ticket-pricing
  spark-event-recommendation
```

例如，票价预测和活动推荐可以分别配置提交权限、ServiceAccount/IAM 身份、ResourceQuota、并发上限和成本标签。namespace 是业务工作负载的管理边界，不代表环境隔离；不同环境的同一业务仍须部署到各自的 cluster。若单个业务有明确的团队、敏感级别或独立运维边界，可再拆分 namespace；避免按每个短期 Spark job 创建 namespace。

每个 namespace 都应明确：

- 谁能提交作业、查看日志、修改配置和清理资源（Kubernetes RBAC）。
- driver 和 executor 使用什么 ServiceAccount、可访问哪些 S3 bucket/prefix（Workload Identity/IAM）。
- Namespace 的 CPU、内存、Pod 数量和临时存储总预算（ResourceQuota），以及容器级 requests/limits 的默认值或范围（LimitRange）。
- 可使用的节点池、优先级和网络策略。
- 资源标签和成本归属，例如 `team`、`environment`、`job_name`、`owner`。

Namespace 配额限制的是 namespace 中**资源请求/对象总量**，本身不会为它预留物理节点。Quota 总量可以高于当前集群容量，实际运行仍会竞争节点；要保证容量，需要另外做节点容量规划、预留或扩容。环境边界由独立 cluster 建立，各业务 Spark namespace 内的配额用于限制该业务的资源总预算；节点池和 affinity 则控制这些 Pod 可使用的计算节点。

## 2. 环境一致性与测试策略

生产环境由 **Airflow 调度 Spark Job**：Airflow 根据 schedule 或上游数据就绪启动 DAG，DAG task 负责提交并监控 Spark application。开发者提交 Git 变更会触发代码流水线，但 CI 不绕过 Airflow 去生产环境启动 Spark。Job 逻辑和 DAG 可以分别修改、分别做快速测试；验证完整行为时，必须在 test Airflow 中触发受控 DAG run，让 Airflow 按生产路径调度 Spark，并检查 task 状态、Spark 运行状态和数据结果。

### 2.1 Job-only、DAG-only 和联合变更如何发布

要做到“只改 Job 就不发布 DAG”，DAG 代码不能把 Job 版本写死在 Python 文件里。DAG task 在运行时读取**环境级 Job 发布配置**；配置为每个 Job 指定不可变的制品 URI 和 checksum。test 与 production 各有独立配置，单独版本化、审批和发布。Airflow 在 DAG run 开始时解析并记录所用 Job 版本，后续 task 和重试沿用该版本，避免运行中途配置变化导致重试切换代码。

例如，DAG 逻辑保持不变，环境配置分别选择运行版本：

```yaml
# test 环境的发布配置示意
jobs:
  daily_features:
    artifact_uri: gs://ml-artifacts/daily-features/1.8.0/job.zip
    sha256: "<artifact-checksum>"
```

配置文件只是版本选择示意，实际可由受控配置仓库或配置服务提供；它不是 DAG Python 文件，也不能指向可变的 `latest`。Airflow task 读取该配置后，将固定 URI 传给 `spark-submit`。

| 变更类型 | PR/构建 | Test 集成验收（入口始终是 Airflow DAG run） | 生产发布 |
|---|---|---|---|
| **只改 Spark Job** | 测 Job 逻辑并构建新的不可变 Job 制品；不改、不构建 DAG | 把 test 环境 Job 配置更新为候选 URI/checksum；部署配置后，用**现有 DAG**触发受控 run，验证 Airflow 确实调用候选制品且输出正确 | 验收通过后，只晋级 Job 制品并把 production Job 配置更新到该版本；不部署 DAG、不因发布自动运行生产数据 |
| **只改 DAG** | 测 DAG 加载、依赖、参数和调度逻辑并构建 DAG；不重建 Job | 部署候选 DAG，test Job 配置保持已验证版本；触发受控 run 验证编排行为 | 验收通过后，只部署 DAG；Job 版本配置不变 |
| **Job 和 DAG 都改** | 分别测试并构建新的 Job 制品和 DAG | test 配置指向候选 Job 版本，部署候选 DAG；触发受控 run 验证这组版本 | 验收通过后分别晋级两项制品和 production Job 配置 |

```text
Job-only:
PR → Job 单元测试 → 构建 Job vNext → 更新 test Job 配置
   → 现有 DAG 在 test Airflow 运行并验证 vNext
   → 晋级 Job vNext → 更新 production Job 配置（不发布 DAG）

DAG-only:
PR → DAG 加载/结构/单元测试 → 构建 DAG vNext
   → test Airflow 部署 DAG vNext，使用当前已验证 Job
   → 触发受控 DAG run 并验收 → 只晋级 DAG vNext

Job + DAG:
PR 分别测试/构建 → test 配置与 DAG 都更新到候选版本
   → test Airflow 运行并验收该组合 → 分别晋级
```

因此，“Test 环境部署/就绪检查”只确认本次变更的制品/配置已部署，Airflow 可加载 DAG 并读取对应 Job 制品；它不证明数据结果正确。“集成验收”才由 test Airflow 触发 DAG run，验证 Airflow 参数传递、提交和监控 Spark，以及输出数据。生产部署不自动启动生产数据计算；后续生产运行仍由 Airflow schedule、数据就绪规则或经批准的手动 DAG run 触发。

### 2.2 先分清 Job、Spark application 和 DAG

这几个名字容易都被叫作“job”，但它们不是一回事：

| 名称 | 是什么 | 生命周期/例子 |
|---|---|---|
| **Spark Job（作业代码/逻辑）** | 业务团队编写的可执行程序，例如清洗事件、生成特征；代码以 `.py`/ZIP、wheel 或 JAR 发布 | 长期存在、版本化；同一份代码可以反复运行 |
| **Spark application（一次运行）** | 一次 `spark-submit` 创建的 Spark 应用，由 driver 和 executor Pods 执行 | 短暂；完成/失败后退出。相同 Job 对不同日期或参数运行多次，就产生多次 application |
| **Airflow DAG** | 描述工作流由哪些 tasks 组成、依赖顺序、何时触发和失败怎么处理 | 长期存在、版本化；由 Airflow scheduler 加载 |
| **Airflow task instance** | 某个 DAG run 中一个 task 的一次执行，例如“提交用户活动聚合 Spark Job” | 随 DAG run 产生；提交 Spark 的 task 负责创建并等待 application，也可以有校验、传感器或发布等不启动 Spark 的 task |

常见关系是：**一个 Airflow task instance 提交一次 Spark application；一次 Spark application 执行某个版本的 Spark Job 代码。**但这不是严格的一对一代码关系：同一 Spark Job 代码可以被多个 DAG/task 复用，也可以被同一 DAG 针对不同分区、日期或参数多次调用；一个 DAG 通常包含多个 Spark applications，也包含非 Spark tasks。一个 Spark application 也可以在程序内部完成多个转换阶段，因此“逻辑步骤数”不必与 application 数相同。

例如，同一个 `daily_activity_job` 可在每日 DAG 中按日期运行一次，也可由 backfill DAG 对一段日期逐日调用；每次提交都会创建独立的 Spark application。反过来，若 DAG 有“清洗”和“聚合”两个独立 Spark tasks，通常会提交两个 applications，并在两者间检查/传递输出。

### 2.3 Spark Job：代码制品、部署和测试

普通应用通常把代码和运行环境打进镜像；这里假设 Spark runtime 镜像由平台提供。Job 团队发布**Job 代码制品**：Python 主脚本及依赖 ZIP/wheel，或 JVM JAR。制品要有唯一版本，并记录 Git commit 和 checksum。

```text
job.py + Python dependencies
  → CI 测试并打包为 job.zip / wheel
  → 上传到不可变 URI：<artifact-store>/jobs/<job-name>/<version>/job.zip
  → Airflow task 从当前环境的 Job 发布配置解析制品 URI，并固定本次 DAG run 使用的版本
  → spark-submit 创建 Spark application
```

制品 URI 必须能由实际提交方式下的 Spark driver 访问；Airflow worker 的本地临时路径不应作为生产制品位置。DAG 代码不硬编码 Job 版本；环境级 Job 发布配置必须固定指向不可变 URI 和 checksum，禁止隐式使用“最新版”。Airflow 在 DAG run 开始时记录解析出的版本，task 重试继续使用同一版本。Job-only 发布通过更新环境配置选择新版本，不需要发布 DAG。若需要平台镜像未包含的系统级依赖，应由平台团队评估并发布受支持的 runtime 更新。

Job 测试从固定输入数据开始，断言输出数据。例如给去重聚合 Job 输入固定事件，检查输出 schema、记录和业务不变量：

```text
输入 events：                         期望输出：
user_id | event_id | event_type       user_id | view_count | favorite_count
u1      | e1       | view             u1      | 1          | 1
u1      | e1       | view  (重复)
u1      | e2       | favorite
```

先用 Spark local 模式测试 transformations，使用固定输入批次和隔离输出路径，检查 schema、关键记录/聚合、行数、分区和数据质量；重复处理同一批次验证幂等性；注入无效 schema、缺失输入或权限错误，确认失败能被正确报告。需要验证集群运行时，不由开发者或 CI 绕过编排层直接提交生产作业；由 test Airflow 运行受控 DAG，让 Airflow task 按生产方式提交候选 Job 制品。测试 fixture 应版本化，不依赖“最新数据”。

### 2.4 Airflow DAG：定义、部署和测试

Airflow DAG 是另一项独立的代码制品，通常是 Python 文件，保存在 Git 仓库；CI/CD 将它部署到 Airflow/Cloud Composer 的 DAG 部署位置，由 Airflow scheduler 加载。DAG 描述 task、依赖、触发规则、输入批次参数、重试、超时和质量门禁；Spark task 在运行时从环境级 Job 发布配置解析不可变的 Job 制品 URI 和 checksum，但不把 Spark transformations 写成 DAG 的职责。

改 DAG 不等于改 Spark Job。比如调整每日 schedule 或增加上游等待 task，只改 DAG，不需重建 Job 制品；改变过滤/聚合逻辑或输出 schema，只改 Job，DAG 代码可以不变，只需把 test 环境 Job 发布配置改为候选制品来做集成验收，生产配置则在批准晋级时更新。若 Job 输入/输出/参数接口改变，则还要协调修改 DAG 的参数映射或数据契约。快速单元测试验证各自改动；准备晋级的候选组合都要在 test Airflow 中做受控 DAG 集成测试。

对 DAG 本身做的快速测试与验证完整生产调度路径的集成测试不同：

| DAG 测试 | 目的 | 是否启动 Spark |
|---|---|---|
| 文件解析/import、DAG 加载检查 | 发现语法错误、缺失依赖、错误配置 | 否 |
| DAG 结构/依赖测试 | 验证 task ID、上下游关系、触发规则和默认参数 | 否 |
| Task 单元测试 | 测试输入批次解析、Spark submit 参数构造、状态/质量门禁分支 | 否；mock 提交接口 |
| Airflow 集成测试 | 在 test Airflow 触发 DAG run，由 Airflow task 提交并等待 Spark application；验证成功/失败传递和最终数据 | 是，在 test Airflow 和 test Spark cluster |

PR 中的 DAG 加载测试可用 Airflow `DagBag` 导入仓库的 DAG 文件；然后用 pytest 检查 DAG 是否存在、是否有 import error、task 集合和依赖是否符合预期，以及 schedule、`catchup`、重试和默认参数等配置。比如：

```python
from airflow.models import DagBag


def test_daily_features_dag_contract():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert dag_bag.import_errors == {}

    dag = dag_bag.get_dag("daily_features")
    assert dag is not None
    assert set(dag.task_ids) == {
        "wait_for_input",
        "validate_input",
        "submit_spark",
        "validate_output",
    }
    assert dag.get_task("validate_input").upstream_task_ids == {"wait_for_input"}
    assert dag.get_task("submit_spark").upstream_task_ids == {"validate_input"}
    assert dag.get_task("validate_output").upstream_task_ids == {"submit_spark"}
    assert dag.schedule == "0 2 * * *"
    assert dag.catchup is False
    assert dag.default_args["retries"] == 2
```

示例里的 DAG ID、task ID 和配置值应替换为项目实际约定；若任务参数是由 helper 生成，可对 helper 单独做单元测试，检查 Job 配置解析、输入日期/分区、namespace 和输出位置。DAG 文件在 import 时不应连接生产系统或启动 Spark；测试可用测试配置或 mock 检查参数构造。此类 PR 测试只验证 DAG 定义和本地逻辑，不会证明 Airflow scheduler、Kubernetes 凭证或 Spark 集群实际可用。合并到 `master` 后，CI/CD 只部署本次有改动的 DAG、Job 制品或环境级 Job 发布配置，再请求 test Airflow 使用候选组件和未变更的已验证组件运行受控 DAG；集成测试的入口是 Airflow DAG run，而不是 CI 中单独调用 `spark-submit`。生产 DAG 的 schedule、数据就绪触发规则和生产连接配置应与 test 隔离，并通过配置/环境管理，不在代码中硬编码凭证或集群敏感信息。

### 2.5 集成测试：在 test Airflow 中验证完整运行路径

单个 Job 测试通过、DAG 结构测试通过，仍不能证明生产调度链路正确。集成测试要部署到 test Airflow/Composer 并触发受控 DAG run；由 Airflow 执行与生产相同的 task/operator 和提交、监控流程，在 test Spark cluster 的业务 namespace 中运行固定输入批次，写入独立输出前缀。测试不能只直接运行 Spark Job，因为那样没有验证 Airflow 如何传参、提交、等待和处理失败。

```text
Airflow DAG
  → 校验固定输入 schema/manifest
  → Job A：清洗（Spark application）
  → 检查 A 的输出
  → Job B：聚合/特征生成（Spark application）
  → 检查最终输出和质量指标
  → 发布到 test 输出位置
```

集成测试应覆盖：Airflow 是否选择了预期 Job 制品版本并传入正确日期、输入和输出参数；上游依赖未满足时是否不启动下游 task；Spark application 未完成时 Airflow task 是否持续等待；Spark 失败是否传回 Airflow 并触发正确的重试/失败分支；超时或取消是否停止对应 application；Airflow retry 是否固定同一输入批次且不会遗留并发重复 application；质量门禁失败是否阻止发布；重复 DAG run 和部分输出后的重跑是否安全；缺失/延迟分区及 schema 不兼容是否按预期失败或跳过。最终检查 Spark 输出契约、数据质量和 DAG 的终态。staging 再用获批的代表性/脱敏批次验证运行时间、资源、质量阈值和告警链路。用 DAG run ID、task instance、Spark application ID 和输入批次 ID 关联任务、日志和输出。

### 2.6 从 Git 到生产：分别晋级 DAG 和 Job

```text
PR
  → CI：只运行受影响组件的测试（Job 逻辑测试和/或 DAG 解析/结构测试）
  → merge 到 master
  → 只改 Job：构建候选 Job 制品并更新 test Job 发布配置
  → 只改 DAG：构建/部署候选 DAG，Job 发布配置不变
  → 两者都改：分别构建/部署候选 Job、DAG，并更新 test Job 发布配置
  → test Airflow：运行现有/候选 DAG 与对应候选/已验证 Job 的兼容组合
  → release/审批：只晋级通过验收的变更；Job-only 同步晋级 Job 制品和 production Job 配置
  → production 审批：只部署获批的 Job 制品、Job 配置和/或 DAG（不自动运行生产数据）
  → Airflow schedule/上游数据就绪：触发生产 DAG
```

Job 制品、DAG 和环境级 Job 发布配置是三个可独立变更的对象。Job-only 发布不发布 DAG：先发布不可变 Job 制品，再只更新 test Job 配置指向候选 URI/checksum，之后让 test Airflow 的**现有 DAG**运行验收；通过审批后晋级 Job 制品，并把 production Job 配置改到同一不可变 URI/checksum。DAG-only 发布不改 Job 制品或 Job 版本配置；候选 DAG 用当前已验证 Job 验收。两者同时变更时，在 test 环境组合验收后分别晋级。更新 production Job 配置或部署 DAG 都不应隐式启动全量 DAG；生产计算仍由 Airflow schedule、上游数据就绪规则或明确批准的手动 DAG run 触发。每次 run 记录 Job checksum/version、DAG version、配置版本、输入批次和输出版本；task retry 固定使用该 run 已记录的 Job 版本。配置更新不得改变已启动 DAG run 的版本选择。若质量门禁失败，停止发布输出并告警；Job/DAG 代码各自回滚到上一个已验证版本，数据回滚依赖版本化输出和显式发布指针，不要盲目覆盖或删除数据。

## 3. 如何分配计算资源

### 3.1 Cluster、Namespace、Node Pool 与 Affinity 各管什么

可以把 Kubernetes 想成“共享机房”：

- **Namespace** 是租户/项目的逻辑边界，提供 RBAC、ResourceQuota 和对象命名隔离。
- **Node pool** 是实际提供 CPU、内存、磁盘和实例类型的计算资源池。
- **Node affinity（节点亲和性）**根据**Node 上的标签**筛选 Spark Pod 可以调度到的节点。Spark 机器不必是特殊型号：只有在需要把 Spark 限定到某个节点子集时，才需要给该子集打标签并设置 affinity；如果集群节点通用、无需隔离，通常不必设置 node affinity，Kubernetes 会按 requests 和其他调度条件选择节点。需要专用节点池时，可给节点打 `workload=spark` 标签，并让 executor 使用 `requiredDuringSchedulingIgnoredDuringExecution` 要求该标签；标签不存在或没有足够可用节点时，Pod 会保持 Pending。`preferredDuringSchedulingIgnoredDuringExecution` 只表达偏好，资源不足时仍可能调度到其他符合条件的节点。Node affinity 选择节点，不会创建节点或预留 CPU/内存；要限制其他工作负载进入特定节点，还需配合 taint/toleration。
- **Pod affinity/anti-affinity（Pod 亲和/反亲和）**根据**其他 Pod 的标签**及其所在拓扑域来决定调度位置。Affinity 表示尽量/必须靠近符合 selector 的 Pod，例如把需要本地缓存的消费者与缓存 Pod 放在同一节点；anti-affinity 表示尽量/必须避开符合 selector 的 Pod，例如让同一次 Spark application 的 executor 分散到不同 hostname。`topologyKey: kubernetes.io/hostname` 表示以节点为分散/聚合边界；用 `topology.kubernetes.io/zone` 则以可用区为边界。软规则 `preferred...` 尽可能满足，硬规则 `required...` 不满足就不调度；硬 anti-affinity 可能因节点数、可用区或资源不足造成 Pending。Affinity 只影响调度位置，不保证 Spark 应用一定能容忍 executor 丢失，也不等同于高可用。
- **Scheduler** 综合 Pod 的 requests、节点标签、taint/toleration、node affinity 和 pod affinity/anti-affinity 进行调度。
- **ResourceQuota** 约束某 namespace 总共可以请求多少资源；它不自动建立专属节点池，也不保证有对应空闲容量。

**本方案的默认资源布局**：dev、test、staging 和 production 各使用独立 cluster；每个 cluster 按业务配置 Spark namespace。Node pool 是否需要专用于 Spark，取决于资源规格、隔离、容量及成本要求；没有这些需求时，Spark 可以和其他工作负载共享通用节点。只有存在专用/受限节点池时，才用 node affinity 路由 Spark Pods；各业务 namespace 的配额和作业并发限制控制该业务的资源总量。

Affinity 是可选的调度规则，不是容量本身：node affinity 不能凭空创建节点，pod anti-affinity 也可能因为没有足够节点而让 Pod Pending。确有隔离或拓扑分散需求时，才结合集群 autoscaler/节点容量、Pod requests/limits、ResourceQuota 和可接受的调度弹性设计；将必需规则（`requiredDuringScheduling...`）用于必须满足的约束，将偏好规则（`preferredDuringScheduling...`）用于软性优化，避免无谓阻塞。

#### 可选示例：Spark executor 的 Node Affinity 与 Pod Anti-Affinity

**这不是 Spark 的必需配置。**Spark 不要求专用或不同型号的机器；如果该环境的 Spark 与其他工作负载共享通用节点、无需节点级隔离，就省略 node affinity 和 `workload=spark` 标签。只有决定建立专用 Spark 节点池（例如要隔离资源争用、使用特定 CPU/内存/Spot 规格，或单独扩缩容）时，才由平台/集群管理员在 node pool 配置（最好以 IaC 管理）中给该节点池添加稳定标签。不要只给当前几台节点手工打标签：节点替换或自动扩缩容后，新节点也必须自动带上该标签。本方案的环境已由不同 Kubernetes cluster 隔离，因此不需要再用节点标签表达 dev/test/prod。

```text
节点池中的节点标签示意：
  workload=spark
  instance-class=general
```

下面是一个**可选的 executor Pod template** 概念片段：只有确有专用 Spark 节点池时才保留 `nodeAffinity`；若共享通用节点，应删除该 `nodeAffinity` 部分。`podAntiAffinity` 也是可选项：它尝试将同一次 application 的 executor 分散到不同 hostname，减少单节点成为集中故障点的机会，但不是 Spark 正常运行的前提。启用该策略时，Airflow DAG 中负责启动 Spark application 的 task/operator 在每次启动前生成一个短唯一 ID（例如随机 UUID 的前 12 位十六进制字符），并用同一个值渲染该次运行专用的 executor Pod template，同时通过 `spark.kubernetes.executor.label.spark-run-id` 给所有 executor Pod 加上该标签。Spark/Kubernetes 不会自动生成这个值。Airflow 若因失败而重新提交 application，应为新提交生成新 ID。不要使用固定业务名，否则不同运行会互相匹配。Spark Pod template 的合并方式及标签配置需按使用的 Spark 版本验证。

```yaml
spec:
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
          - matchExpressions:
              - key: workload
                operator: In
                values: ["spark"]
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 80
          podAffinityTerm:
            topologyKey: kubernetes.io/hostname
            labelSelector:
              matchLabels:
                spark-run-id: "<unique-id-for-this-application>"
```

这里确实是**Pod 反亲和**：判断依据是它位于 `podAntiAffinity` 下面。Kubernetes 在 `podAffinity` 和 `podAntiAffinity` 两种规则中都使用字段名 `podAffinityTerm` 来描述拓扑范围和匹配的 Pod；这个字段名不会把规则变成 affinity。

Airflow task/operator 启动 application 时，把已用本次 `run-id` 渲染好的 executor template，以及同一个 `run-id` 标签一起传给 `spark-submit`（参数和 template 文件位置需按所用 Spark 版本、提交模式调整）：

```bash
spark-submit \
  --conf spark.kubernetes.executor.podTemplateFile=<executor-pod-template-path> \
  --conf spark.kubernetes.executor.label.spark-run-id=<unique-run-id> \
  ...
```

上例中的 `<unique-run-id>` 是 Airflow task/operator 为这次 Spark application 生成、并同时写入 Pod template selector 的同一个值，不是照抄的字面字符串。它应符合 Kubernetes label value 规则；每次提交产生一个 ID，并将该次 application 的所有 executor 设为同一标签值。`<executor-pod-template-path>` 必须能由 Spark 按当前提交模式读取；不要假设 Airflow worker 上的任意临时路径会自动出现在 Spark driver/executor 容器中。

若配置了专用 Spark 节点池，`requiredDuringScheduling...` 代表没有带 Spark 标签的节点时宁可 Pending，也不调度到通用节点；若允许回退到其他节点，可改为 `preferred...`。示例中的 pod anti-affinity 是软约束：节点空闲容量充足时尽量把同一运行的 executor 分散到不同 hostname，但集群紧张时仍可共置。若改为硬约束，同一运行的 executor 数量不能超过满足资源条件的可用节点数，否则会有 executor Pending。若目标是跨可用区分布，可把拓扑键改为 `topology.kubernetes.io/zone`，但需有足够节点和容量。分散 executor 不能代替 Spark 自身的失败恢复和应用级重试策略。

**Pod affinity（靠近）**适合需要与特定服务或本地缓存共置的场景。例如，若每个节点运行一个带标签 `role=local-cache` 的缓存 Pod，可让某个消费者 Pod 尽量调度到同一节点：

```yaml
spec:
  affinity:
    podAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 50
          podAffinityTerm:
            topologyKey: kubernetes.io/hostname
            labelSelector:
              matchLabels:
                role: local-cache
```

Pod affinity 的 selector 默认在同一 namespace 内查找匹配 Pod；跨 namespace 时应明确配置 namespace/namespace selector，并确认 RBAC 与安全边界。对 Spark 批处理，通常不需要为了“靠近 driver”而设置 Pod affinity：driver 与 executor 通过 Spark 网络通信，强制共置反而会限制调度并造成资源争用。Pod affinity 应用于有明确本地性收益的情况；anti-affinity 应按单次运行标签和目标故障域设计。

Node/Pod affinity 只表达“放在哪里”，不表达“要多少 CPU/内存”。Spark executor 数量、每个 executor cores/memory、Kubernetes requests/limits、shuffle 临时盘和 namespace quota 仍须分别配置。

### 3.2 给 Spark 作业设资源预算

提交前按作业估算总资源。静态 executor 数量下的粗略计算是：

```text
作业 CPU 约等于 driver CPU + executor 数量 × 每个 executor 的 CPU request
作业内存约等于 driver 容器内存 + executor 数量 × 每个 executor 容器内存
```

实际容器内存还包括 JVM overhead、Python worker、off-heap、shuffle 和本地临时存储开销，不能只按 `spark.executor.memory` 计算。应同时设置/检查 Kubernetes Pod requests/limits、Spark driver/executor memory 与 cores，并在压力测试后留出节点和系统组件的余量。

资源控制分两层配置：

- **Namespace 层：** `ResourceQuota` 设置该 namespace 内所有 Pod 的 requests/limits 总额、Pod 数量等上限，是 namespace 的总预算，不是单个作业或 Pod 的额度。
- **容器层：** 在 Spark 生成的 driver/executor Pod 中，为容器设置 CPU/内存 `requests` 和 `limits`。`requests` 用于调度与资源预留计算；`limits` 是容器可使用资源的上限（CPU 通常会被限速，内存超限可能触发 OOMKilled）。Spark 可通过作业的 Spark 配置设置相应资源，也可用 Pod template 配置；需核对 Spark 版本生成的 Pod spec，避免 Spark 配置覆盖 template 字段。
- **Namespace 默认/范围策略：** `LimitRange` 可为未显式填写的容器资源设置默认 requests/limits，也可限制单个容器允许的最小/最大值。它不替代 `ResourceQuota`，且生产作业最好在 Spark 提交配置中明确声明资源，而不是依赖隐式默认值。

例如，一个 Spark application 会创建一个 driver Pod 和多个 executor Pods；每个 Pod 的容器资源值分别计入其业务 namespace 的 quota。票价预测的 quota 限制该业务在当前环境 cluster 内的总消耗；活动推荐则由自己的 namespace quota 管理。namespace 总 quota 与单个容器 limits 是不同维度，通常两者都要配置。ResourceQuota 示意：

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: spark-quota
  namespace: spark-ticket-pricing
spec:
  hard:
    requests.cpu: "20"
    requests.memory: 80Gi
    limits.cpu: "40"
    limits.memory: 120Gi
    pods: "100"
```

这个数值只是 YAML 示例，不是通用建议。启用 CPU/内存配额时，Pod 中的容器必须提供相应的 requests 或 limits；可用 LimitRange 设置容器默认值和单容器范围。还要在容器资源配置及 namespace quota 中考虑 ephemeral storage，避免 shuffle spill 把节点磁盘耗尽。

### 3.3 资源池和优先级的实用策略

- 在每个环境 cluster 的 Spark namespace 中设定资源预算；dev/test 可设较低的 ResourceQuota 和更严格的并发上限。
- 如确有隔离、硬件规格或独立扩缩容需求，可在生产 cluster 内设置专用 Spark node pool，并通过 node affinity 和 taint/toleration 路由 driver/executor；否则可与其他工作负载共享通用节点。
- 使用 pod anti-affinity 或 topology spread 将 executor 分散到不同节点/可用区；资源有限的 test 环境以 preferred 软规则为主，避免硬性约束导致长时间 Pending。
- 通过多个小作业的并发上限避免测试作业占满 driver/executor 配额；生产重要作业可用独立调度策略或节点池。
- 对可抢占/可中断的测试任务使用较低 PriorityClass；生产优先级要谨慎设置，避免所有作业都标成最高级。
- 使用 node affinity/selector 和 taints/tolerations 管理 CPU、内存优化型或 Spot 节点；容错性差、长时间 shuffle 的关键生产作业要谨慎使用会被回收的节点。
- Spark Dynamic Allocation 可按工作负载伸缩 executor，但要设置合理的最小/最大 executor 数，并验证 Spark 版本、shuffle 保留方案及 Kubernetes 调度行为。动态分配不是集群扩容的替代品。
- 对超大批处理作业评估 gang scheduling（例如与 Spark 版本兼容的调度器），减少只分到部分 executor 后长期等待的情况；引入前先测量，因为它会影响其他 Pod 的公平性和调度策略。

## 4. 监控什么、在哪里看

监控要同时覆盖 **Kubernetes、Spark 应用、数据结果和业务 SLA**，仅看 Pod 是 Running 不够。

| 层面 | 关键指标/事件 | 常见来源 |
|---|---|---|
| Kubernetes/节点 | Pod Pending、OOMKilled、重启次数、CPU/内存/临时盘使用、节点压力、配额拒绝、镜像拉取失败 | Kubernetes events、Metrics Server、Prometheus/Grafana、云监控 |
| Spark 应用 | 作业状态/耗时、active/failed tasks、stage 重试、executor 数、GC、shuffle read/write、spill、失败原因 | Spark UI、Spark REST API、Spark metrics |
| 作业数据 | 输入/输出行数、文件数/大小、空值率、重复率、分区完整性、迟到数据、质量检查失败 | 作业日志和质量检查结果，写入数据平台监控 |
| 业务 SLA | DAG 是否按时开始/结束、上游数据新鲜度、重试后是否超时、生产发布是否成功 | Airflow/编排器告警、on-call 通知 |

### 4.1 保留历史 Spark UI 和日志

Spark driver Pod 结束后，运行中的 UI 不再适合追查历史。因此开启 Spark event logging，将 event log 写入 driver 可写、History Server 可读的持久目录（例如受限的 S3 前缀），部署 Spark History Server 查看已完成作业。生产上保护 History Server 的访问权限，并配置日志保留和清理策略。

同时集中收集 driver/executor stdout/stderr 和 Kubernetes events。日志中避免输出凭证、敏感行数据或完整业务 payload；用 `application_id`、`run_id`、image digest 和数据批次 ID 串联日志、指标与 DAG 运行。

### 4.2 告警要面向行动

优先为以下情况设置告警和负责人：

- 生产 DAG 未在 SLA 前完成、反复重试或没有新鲜输入。
- Driver/executor 长时间 Pending、CrashLoopBackOff、OOMKilled。
- 作业完成但输出行数/分区为零或质量门禁失败。
- executor 丢失、stage/task 重试暴增、shuffle spill 或数据倾斜超过基线。
- 资源配额持续打满、节点 CPU/内存/磁盘压力导致排队。

告警应链接到对应 DAG run、Spark application、History Server 页面和排障手册，而不只是发送一条无上下文的“作业失败”通知。

## 5. 任务系统如何调度 Spark Jobs

### 5.1 任务系统选型：现代系统还用 Airflow 吗

**Airflow 仍是现代批处理数据流水线的主流选择之一，但不是唯一选择，也不是 Spark 的必需组件。**Apache Airflow 当前定位就是开发、调度和监控有明确开始/结束的批处理工作流；GCP 也提供基于 Apache Airflow 的 Managed Airflow（Cloud Composer）。因此，如果团队已经使用 Airflow/Composer，或需要在多个数据 Job 之间表达复杂依赖、定时和数据就绪触发、重试、backfill、告警与可视化，继续用它编排 Spark jobs 是很自然的方案。

不过，Spark 本身不要求 Airflow。若只是少量简单批处理，可以选择云平台的原生调度/工作流服务；若组织已有统一的其他编排平台，也可由其调用 `spark-submit` 或相应的 Spark 提交接口。选型看的是**谁负责数据流水线的业务级触发、依赖、状态和重试**，而不是 Spark 计算引擎。不要让 Airflow、Kubernetes CronJob 和另一个 scheduler 同时无协调地触发同一份生产工作流。

在 GCP 上，若团队选择 Airflow，通常使用 Cloud Composer 托管 Airflow 控制面；DAG 由 CI/CD 部署到 Composer 环境，Airflow scheduler 加载并运行。Composer 环境本身运行在 GKE 上，但它与本文的 Spark workload clusters 是不同职责：Composer 负责编排，Spark Kubernetes cluster 负责运行每次 Spark application。Airflow worker/task 需要通过受控身份访问目标 Kubernetes API 并提交/监控作业；不要将 Airflow DAG 或 scheduler 放进 Spark application，也不要把 Airflow 自身的存活当成 Spark job 的运行状态。

**本文后续以 Airflow/Cloud Composer 为例说明**，因为它和批处理 DAG、Spark jobs 的编排模型相匹配；若实际采用其他任务系统，只需替换编排层，Spark application 的生命周期和数据 Job 测试原则不变。

### 5.2 职责分层

- **Airflow（任务/工作流系统）**：保存 DAG 定义，按日历或上游数据就绪触发 DAG run；管理 Job 之间的依赖、批次/data interval、重试、超时、质量门禁、告警和发布顺序。Airflow task 是一次有开始/结束状态的编排步骤，不是长期运行的 Spark 服务。
- **`spark-submit` 提交步骤**：Airflow task 调用受控的提交脚本/命令，带上 Job 制品 URI、平台 Spark runtime 版本、Kubernetes cluster/namespace、资源参数、输入批次、输出 run 路径和唯一运行 ID，创建一个 Spark application。
- **Spark on Kubernetes**：Kubernetes 启动该 application 的 driver Pod；driver 再按需创建 executor Pods 执行计算。它们是本次运行的临时工作负载，不是一个长期常驻的 Spark Master/Worker 服务。成功或失败后，Airflow 记录终态；Pods 按保留策略留作诊断或清理。
- **Kubernetes scheduler**：只负责把 driver/executor Pods 放到符合资源和调度约束的节点上；它不负责决定业务 DAG 顺序、数据批次或何时重跑整个数据 Job。

**DAG 定义在 Airflow，不定义在 Spark 里。**DAG 通常以 Python 文件和配置保存在作业团队的 Git 仓库，经 CI/CD 部署到 Airflow 的 DAG 部署位置，由 Airflow scheduler 加载。Airflow 自身运行在编排/控制环境中，可以独立于 Spark Kubernetes cluster；Airflow task 通过有权限的提交环境访问目标集群 Kubernetes API，再用 `spark-submit` 创建 Spark application。Spark 只执行被提交的单个 Job，不负责读取或运行 Airflow DAG。Airflow 与 Spark 可以部署在同一个 Kubernetes cluster，也可以分开部署；本文建议的环境集群隔离下，需为 Airflow 配置到各目标 cluster 的受控连接和身份。

因此，调度是两层但职责不同：Airflow 决定**何时运行哪个 Job、依赖什么输入、失败后如何重试**；Kubernetes 决定**这次运行创建的 Pods 在哪里执行**。每个数据流水线只指定一个业务级主调度系统（此方案为 Airflow），不要再用 Kubernetes CronJob 或其他 scheduler 无协调地重复触发同一生产 DAG。

### 5.3 一个批处理 DAG 的典型步骤

```text
Airflow schedule / 上游数据就绪
  → 创建 DAG run，固定 data interval / 输入批次 / run_id
  → 校验输入 manifest/schema
  → Airflow task 提交 Spark application
       ├─ Kubernetes 创建 driver Pod
       └─ driver 创建 executor Pods 并运行计算
  → 提交 task 监控 application，直到成功/失败/超时
  → 成功：校验输出 manifest、分区和质量指标
  → 质量检查通过：发布数据版本/更新逻辑指针
  → 失败：按策略重试或停止，传播失败并告警
```

Airflow 必须能判断 Spark application 的真实终态，不能把“提交请求成功”误认为 Spark 作业成功。提交与监控可以由同一个 Airflow task 完成，也可以拆成 submit/wait 两个 task；无论如何都要保存 Kubernetes application/driver 标识、持续观察终态，并让 Spark 失败状态最终使 Airflow task 失败。不要让 Airflow task 在 driver 仍运行时提前成功或丢失对该 application 的跟踪。

重试需要设计成幂等：输入固定到 DAG data interval/批次，输出写入唯一的 run/version 前缀；质量检查通过后才发布。不要在重试中偷偷读取“最新可用数据”，否则同一次 DAG run 可能读到不同输入。

简化的 Airflow DAG 结构示意（operator API 随 Airflow/provider 版本变化）：

```python
wait_for_input >> validate_input >> submit_spark_app
submit_spark_app >> validate_output >> publish_output
```

`submit_spark_app` 是执行 `spark-submit` 的 Airflow task（例如在 Kubernetes Pod 中运行）。该 task 应等待 Spark application 终态、将失败状态传回 Airflow，并在取消或超时时取消/清理仍在运行的 application，避免 Airflow task 已结束但 Spark driver 仍在运行。生产实现还应处理 Airflow worker 重启后的状态恢复：通过稳定的 DAG run/task attempt 标识重新定位已有 driver/application，避免盲目再提交一份重复计算。

### 5.4 重试、取消和重复触发

- Airflow task retry 与 Spark task retry 是不同层级：Spark 内部重试 task/stage；Airflow 重试可能重新提交整个 Spark application。设置重试前，明确旧 application 已终止或可被安全复用，避免一次逻辑运行并发产生多个 Spark application。
- 提交前为每次逻辑运行生成稳定的 `run_id`/幂等键，并确认重复提交时是复用、拒绝还是另开版本，不能产生重复业务写入。
- 设置 Spark 作业最大运行时间、Airflow execution timeout 和取消传播；DAG 超时/手动取消后，应停止对应 Spark application，并确认 driver/executor 不再运行。按保留策略保留必要日志和失败现场，再清理 Pod/临时资源。
- 限制 Airflow DAG/task 并发、每个业务 namespace 的 Spark application 并发及 executor 上限；扩缩容由 Kubernetes/Spark 配置按额度执行。Airflow 控制作业级并发，ResourceQuota 控制 namespace 总资源请求，二者配合避免某个 DAG 填满集群。
- 对有状态流式作业，部署/升级通常还要管理 checkpoint/savepoint 和状态兼容；不要把普通批处理的“失败重跑”策略直接套用。

## 6. 推荐落地方案

一个易于起步且边界清晰的布局（每个环境各有一个 cluster；namespace 按业务划分）：

```text
dev Kubernetes cluster
  spark-ticket-pricing
  spark-event-recommendation

test Kubernetes cluster
  spark-ticket-pricing
  spark-event-recommendation

staging Kubernetes cluster
  spark-ticket-pricing
  spark-event-recommendation

production Kubernetes cluster
  spark-ticket-pricing
  spark-event-recommendation
```

环境隔离通过独立 cluster 实现；每个业务 namespace 独立配置权限、ServiceAccount/IAM、ResourceQuota、并发限制和告警归属。是否建立 Spark 专用 node pool、配置 node affinity 或 pod anti-affinity，应根据资源隔离、硬件、扩缩容和故障域目标决定，不作为 Spark 的默认必需条件。需要强故障域隔离时，应确认标签、taint/toleration、可用节点数、PodDisruptionBudget 和实际 Spark Pod template 都共同满足要求。

发布链路固定为“test cluster 验证作业代码制品及其 Spark 运行时兼容性 → staging cluster 验证 → 审批晋级同一作业代码制品到 production cluster”；监控上至少具备 Kubernetes 指标、Spark event logs/History Server、编排器告警和输出质量门禁。依据真实作业的 CPU、内存、shuffle、运行时、Pending 时间和节点利用率调整生产节点池及调度规则，不要仅凭静态 executor 数量猜容量。

## 参考资料

- [Apache Spark: Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)：Kubernetes 部署、资源调度、namespace、RBAC、Dynamic Allocation 和应用管理。
- [Apache Spark: Monitoring and Instrumentation](https://spark.apache.org/docs/latest/monitoring.html)：Spark UI、event log、History Server 和 metrics。
- [Apache Airflow: What is Airflow?](https://airflow.apache.org/docs/apache-airflow/stable/index.html)：Airflow 对批处理工作流的定位、DAG、调度和监控。
- [Google Cloud: Managed Airflow overview](https://cloud.google.com/composer/docs/composer-3/composer-overview)：Cloud Composer/Managed Airflow 的 GCP 托管架构及 DAG 管理方式。
- [Kubernetes: Resource Quotas](https://kubernetes.io/docs/concepts/policy/resource-quotas/)：Namespace 资源配额和配额行为。
- [Kubernetes: Resource Management for Pods and Containers](https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/)：requests、limits 和容器资源管理。
- [Apache Airflow: Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)：任务幂等、数据区间、重试和远端数据传递。
