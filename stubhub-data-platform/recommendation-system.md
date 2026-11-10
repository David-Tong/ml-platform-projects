# StubHub 推荐系统

推荐系统结合地点感知的候选召回、协同过滤和业务重排。候选活动由 Elasticsearch 根据用户当前地点检索；离线 ALS 模型提供用户/活动表示及协同过滤分数；精排综合 ALS 分数和其它特征，最后纳入广告及业务规则进行重排。这里的 ALS 是 **Alternating Least Squares（交替最小二乘）**。文中的表名、权重和例子都是方案示意，并非 StubHub 生产配置的确认记录。

## 1. 在线推荐流程

1. 用户请求推荐时，应用将用户标识和当前地点传给推荐服务。
2. 推荐服务使用 Elasticsearch 按地点搜索当前有效的活动，形成候选集；索引应提供足以过滤过期、不可售活动的状态信息。
3. 推荐服务取得离线训练生成的用户向量和活动向量，并计算候选活动的 ALS 协同过滤分数。
4. 精排模型综合 ALS score 与其它用户、活动及上下文特征，对召回候选打分并排序；下文以 GCP 上训练和部署的 XGBoost 排序模型为例。
5. 最终重排阶段加入广告及其它业务需求，生成结果并返回给用户。

用户切换地点后，地点搜索会生成相应的候选活动，精排和重排也可使用当前请求上下文；不需要为每个地点预先物化最终推荐列表。广告和业务规则属于最终重排逻辑，不是 ALS 模型学出的偏好。

## 2. 离线训练与数据

ALS 输入为用户—活动交互强度 feed，由浏览、收藏、购买等历史行为按用户、活动和统计时点汇总而成。DataProc/Dataproc 上的 Spark ALS 批处理学习用户向量和活动向量；交互数据应限定到明确的 `snapshot_date`，避免训练使用当时尚未发生的行为。训练产物及其数据、代码、参数版本需要随发布记录。

ALS 分数是用户向量与活动向量的匹配信号，不是点击概率或购买概率。精排模型还需要用户/活动属性、地点和请求上下文等特征，以及历史曝光及其后续点击、收藏、购买等结果作为监督样本。未曝光活动不能直接当作用户看过后拒绝。

ALS 的输入、矩阵分解原理、隐式反馈和适用范围详见 [ALS 算法参考](./ref/als-recommendations.md)。核心原则是：ALS 从用户—活动交互中学习用户和活动向量；其分数只是精排信号，不是点击/购买概率，也不包含请求时变化的地点。按地点召回由 Elasticsearch 完成，精排结合 ALS score 和其它特征，最后再执行广告与业务重排。

## 3. XGBoost 在推荐中负责什么

本文以 **2019 年**为技术时间背景，使用当时的 **AI Platform Training / Prediction（前身为 Cloud ML Engine）**。这里选择 **XGBoost Learning to Rank 作为精排模型**，属于可实现的设计示例，不代表已确认的 StubHub 生产配置。树模型原理见 [XGBoost 入门](./ref/xgboost.md)。

| 阶段 | 实现 | 输入 → 输出 |
|---|---|---|
| 地点召回 | Elasticsearch | 当前地点、有效活动条件 → 候选活动 |
| 协同过滤信号 | Dataproc / Spark ALS | 历史交互 → 用户/活动向量及匹配分数 |
| 精排 | XGBoost 原生 `Booster` 排序模型 | 每个候选的 ALS score、用户、活动、上下文特征 → 相关性分数 |
| 最终重排 | 推荐服务中的策略逻辑 | 精排结果 → 满足广告、去重和业务约束的列表 |

例如精排输入可以包含 `als_score`、`distance_km`、`hours_to_event`、活动热度、可售库存、最低票价和用户类别偏好。类别字段需通过训练时确定的编码转换；用户 ID、活动 ID 和请求 ID 不直接当连续数值。冷启动用户或活动没有 ALS 向量时，使用约定的缺失值及标志，并评估相应切片。

排序模型输出的是用于同一请求内比较的分数，不是点击或购买概率。如果业务需要概率，可另建以曝光后点击为标签的 `XGBClassifier(objective="binary:logistic")` 基线，并检查校准；不能把排序分数直接当概率。

### 3.1 训练样本：同一请求中的活动放在一组

一行是一条历史有效曝光，`request_id` 标识同一次推荐请求；按事先定义的归因窗口关联后续行为。示例使用点击标签 `relevance=1/0`，也可设计多级相关性，但点击、收藏、购买之间的等级需要业务验证。

| request_id | 活动 | als_score | distance_km | hours_to_event | relevance |
|---|---|---:|---:|---:|---:|
| R1 | A | 0.8 | 5 | 48 | 1 |
| R1 | B | 0.6 | 20 | 72 | 0 |
| R2 | C | 0.3 | 2 | 24 | 0 |
| R2 | D | 0.7 | 8 | 96 | 1 |

只用标签窗口已经结束的曝光；没有召回或没有展示的活动不自动成为负样本。曝光位置、原有排序和流量来源会影响标签，需记录这些信息并评估偏差，不能认为排序目标自动消除了位置偏差。

BigQuery 按请求时间关联当时可用的特征，冻结训练、验证、测试分区。同一请求不能拆到多个集合；ALS 向量也必须来自该请求时点之前可用的模型，不能用未来交互训练出的向量回填历史。记录 `als_model_version`，避免精排训练与线上使用的 ALS 分数分布不一致。

### 3.2 排序训练代码的核心部分

以下使用 2019 年 XGBoost 0.82 / 0.90 系列已有的原生 `DMatrix`、`set_group` 和 `xgb.train` 接口。实际版本需与所选 AI Platform 预测 runtime 匹配。假设 `train`、`valid` 已加载为 DataFrame，特征已转换为数值且标签通过检查；这不是完整的云上提交脚本。

```python
import xgboost as xgb

features = ["als_score", "distance_km", "hours_to_event"]

# 每个请求的行必须连续；请求 ID 仅用于分组，不作为输入特征。
def grouped_matrix(frame):
    frame = frame.sort_values("request_id").reset_index(drop=True)
    matrix = xgb.DMatrix(frame[features].values, label=frame["relevance"].values)
    sizes = frame.groupby("request_id", sort=False).size().values
    matrix.set_group(sizes)
    return matrix

dtrain = grouped_matrix(train)
dvalid = grouped_matrix(valid)

params = {
    "objective": "rank:pairwise",
    "eval_metric": "ndcg@10",
    "tree_method": "hist",
    "max_depth": 6,
    "eta": 0.05,
    "seed": 42,
}
trial = xgb.train(
    params, dtrain,
    num_boost_round=500,
    evals=[(dvalid, "valid")],
    early_stopping_rounds=30,
    verbose_eval=False,
)
# 旧版提前停止会保留停止时的全部树。
# 按验证集选出的轮数重新训练，供默认加载全部树的服务使用。
best_rounds = trial.best_iteration + 1
model = xgb.train(params, dtrain, num_boost_round=best_rounds)
model.save_model("model.bst")
```

参数是起点，需要在验证集上选择。`rank:pairwise` 学习同一请求内相关性不同的活动之间的顺序，以 NDCG@10 评估排序效果。预测时对每个候选计算分数后由推荐服务排序，不需要再传标签或训练分组。组内没有相关性差异时无法形成有效排序对，应监控此类请求比例；评估还需说明无正例请求的指标口径。接口参考 [XGBoost 0.90 API 文档](https://xgboost.readthedocs.io/en/release_0.90/python/python_api.html)。

## 4. 在 GCP 上训练和发布 XGBoost

### 4.1 各组件的职责和作业顺序

```text
Airflow 编排
  → BigQuery：曝光、结果与历史特征按时点关联
  → GCS：冻结 train / valid / test 与特征 schema
  → AI Platform Training：运行 Python 训练包
  → 独立测试集评估、产物校验
  → GCS：保存 model.bst 及发布清单
  → AI Platform Prediction：创建 model / version 并部署
  → 推荐服务：批量提交当前请求的候选特征，取得分数后重排
```

Dataproc 继续负责 Spark ALS；XGBoost 可以单独运行在 AI Platform Training 的 CPU 训练机器上，不要求 GPU。单机内存放不下时再考虑经过验证的分布式训练方案，不能只增加 worker 数就假设训练自动分布式执行。

实际执行步骤：

1. 准备 GCP 项目、区域、GCS 数据/产物目录，启用当时的 ML Engine API（`ml.googleapis.com`）。为提交者和平台运行身份配置作业提交、数据读取及产物写入权限；不在代码中保存账号密钥。
2. 把训练代码组织为 Python 包，例如 `trainer/task.py`、`trainer/__init__.py` 和 `setup.py`。锁定 runtime 支持的 Python、XGBoost 和数据处理依赖。训练入口接收 GCS 输入、输出目录及超参数，读取数据后执行第 3.2 节的训练逻辑。
3. 使用当时的 `gcloud ai-platform jobs submit training` 提交训练包，指定 `--package-path`、`--module-name`、`--region`、`--runtime-version`、`--python-version` 和 `--job-dir`，并设置训练资源规格。Airflow 负责提交、轮询任务状态及失败处理。`--job-dir` 是传给程序的输出约定，训练入口仍需显式上传本地 `model.bst`，不能假设 `save_model("gs://...")` 会完成上传。
4. 独立评估 NDCG@K、Recall@K，并与 ALS 排序及简单热度基线比较；按地点、新用户、新活动和 ALS 缺失切片检查。平台负责执行代码，样本定义和发布门槛仍由项目实现。
5. 每次发布在独立 GCS 目录保存 `model.bst`、特征顺序、编码与缺失规则、训练轮数、ALS 版本、数据/代码版本及评估报告。创建 AI Platform 的 model 资源，再创建指向该目录的 version，使用 `XGBOOST` 框架和兼容的 runtime。model/version 是部署资源，发布清单由项目在 GCS 管理。

Google 在 [2018 年的 XGBoost 在线预测介绍](https://cloud.google.com/blog/products/gcp/serving-real-time-scikit-learn-and-xgboost-predictions)中已经说明了 `model.bst → GCS → model/version → 在线预测` 的流程。命令名称随 Cloud ML Engine 更名为 AI Platform 发生过变化，本文按 2019 年 AI Platform 命名描述；runtime 与 Python 组合以当时部署月份的支持列表为准。

### 4.2 在线预测和调用约定

本示例使用 AI Platform Prediction 的内置 XGBoost 服务加载 `model.bst`，推荐后端负责特征处理。后端将类别编码、缺失处理和特征顺序固定为训练版本，再调用 `projects/{project}/models/{model}/versions/{version}:predict`，服务按模型返回候选分数。应在所选 runtime 上验证排序目标的加载与预测兼容性；若不满足要求，可以在 GKE / Compute Engine 上自建 Python 服务，加载同一个二进制模型并调用 `Booster.predict(DMatrix(...))`。

约定一条 `instance` 对应一个候选，输出与输入保持同序。例如同一请求的两个候选一次调用：

```json
{"instances": [[0.8, 5.0, 48.0], [0.6, 20.0, 72.0]]}
```

这三个位置分别对应前文的 `als_score`、`distance_km`、`hours_to_event`。返回 `{"predictions": [0.72, 0.31]}` 是接口形状示例，数值不是运行结果。推荐服务自己保留候选 ID 与行号的映射，按分数降序排列，再执行广告和业务规则。当前距离由请求地点计算，预测服务不负责 Elasticsearch 召回或读取 ALS 向量。

上线前校验本地与云端对同一批特征的分数一致性、批量大小限制、延迟及超时回退；失败时可回退到经过验证的 ALS/热度规则。监控特征时效与缺失、分数分布、P95 延迟和错误率，记录曝光、模型及 ALS 版本。按用户稳定分组路由到明确的 model/version，做 A/B 测试并保留旧版本；切换默认 version 不等于稳定用户级实验。重新发布 ALS 后也要检查精排表现。
