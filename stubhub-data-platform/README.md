# StubHub 数据与机器学习平台文档

本文档集按系统职责拆分为四部分：

1. [数据平台](./data-platform.md)：Airflow 调度数据流水线、数据库入湖与数仓整理、报表和模型供数；埋点处理尚待确认，Pub/Sub 与 Dataflow 作为可选方案说明。
2. [价格预测系统](./price-prediction.md)：使用 GCP ML Platform 训练和提供回归模型预测价格。
3. [推荐系统](./recommendation-system.md)：使用 Dataproc/Spark ALS 学习用户与活动向量，结合 Elasticsearch 地点召回、精排和业务重排。
4. [A/B 测试](./ab-testing.md)：实验分组、曝光与结果采集、分析和发布决策。

文中的表名、字段、指标和模型训练步骤是架构说明或建议，不代表已从 StubHub 生产代码和配置逐项核实。
