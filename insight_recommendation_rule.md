# Insight Recommendation Rule Memory

> Global preference memory for insight push recommendation.
> This file is workspace-global (not project-scoped) and is auto-updated when users click useful/not_relevant.

## 用户偏好规律
- 暂无，等待用户反馈。

## 喜欢的推送实例
- 暂无。

## 否定的推送实例
- 暂无。

## 最近反馈分析

### 2026-03-20T15:59:05 | 项目：金融模型优化 | 反馈：有用
- card_id: insight_1_cd003c3e
- 标题: RAG over Tables: Hierarchical Memory Index, Multi-Stage Retrieval, and Benchmarking
- 来源: arxiv
- 链接: http://arxiv.org/abs/2504.01346v4
- 核心洞察: 表格知识需要分层索引和多阶段检索才能实现高效准确的金融信息提取
- 项目关联: 该研究直接针对表格知识检索问题，与金融知识图谱构建任务高度相关，提供了可落地的技术方案
- 证据摘录: Retrieving knowledge from a table corpora (i.e., various individual tables) for a question remains nascent
- 风险提醒: 引用：'Retrieving knowledge from a table corpora (i.e., various individual tables) for a question remains nascent, at least, for (i) how to understand intra- and inter-table knowledge effectively, (ii) how to filter unnece...
- 替代方向: 考虑采用T-RAG框架的分层内存索引和多阶段检索方法，结合金融领域特点进行定制化改进

### 偏好规律增量
- 用户更倾向于保留能明确说明“为什么与当前项目相关”的推送，尤其是能把外部内容映射到当前任务/目标的卡片。本次高信号点：该研究直接针对表格知识检索问题，与金融知识图谱构建任务高度相关，提供了可落地的技术方案

### 喜欢的推送实例增量
- RAG over Tables: Hierarchical Memory Index, Multi-Stage Retrieval, and Benchmarking | 来源: arxiv | 匹配原因: 该研究直接针对表格知识检索问题，与金融知识图谱构建任务高度相关，提供了可落地的技术方案 | 链接: http://arxiv.org/abs/2504.01346v4

### 否定的推送实例增量
- 暂无新增（待后续负反馈补充）。

### 本次反馈原因分析
- 本次标记为“有用”，说明这张卡片不仅主题相近，还给出了足够具体的项目关联说明。 证据摘录：Retrieving knowledge from a table corpora (i.e., various individual tables) for a question remains nascent

### 2026-03-23T18:07:11 | 项目：多模态技术打磨 | 反馈：有用
- card_id: insight_1_29cc44b5
- 标题: Enhancing Temporal Understanding in Video-LLMs through Stacked Temporal Attention in Vision Encoders
- 来源: arxiv
- 链接: http://arxiv.org/abs/2510.26027v1
- 核心洞察: 通过在视觉编码器中引入堆叠时间注意力模块，显著提升视频-LLMs对时间动态的理解能力，解决当前架构在动作序列理解方面的关键局限
- 项目关联: 该研究直接针对视频理解中的时间动态问题，与项目目标'以长视频深度理解为切入点'高度一致，提出的堆叠时间注意力方法可以显著提升模型在动作序列理解和时间进展方面的能力，有助于实现'细粒度认知推理能力达到SOTA水平'的目标
- 证据摘录: Our experiments show that current Video Large Language Model (Video-LLM) architectures have critical limitations in temporal understanding, struggling with tasks that require detailed comprehension of action sequences a...
- 风险提醒: 引用：'Despite significant advances in Multimodal Large Language Models (MLLMs), understanding complex temporal dynamics in videos remains a major challenge.' (Enhancing Temporal Understanding in Video-LLMs through Stacked...
- 替代方向: 考虑将堆叠时间注意力模块与项目中的'全模态统一Attention机制设计'相结合，构建专门针对视频时间特性的注意力机制，同时保持跨模态统一表征架构的完整性

### 偏好规律增量
- 用户对能明确将技术痛点与项目具体目标（如“长视频深度理解”、“SOTA水平”）建立映射关系的卡片给予高权重。后续筛选应优先保留 `relevance_reason` 中包含项目目标关键词或里程碑描述的内容。（待继续验证，已有2个正样本支持）
- 用户偏好提供具体架构改进方案（如“堆叠时间注意力模块”、“分层索引”）的学术论文，而非仅泛泛讨论趋势。后续排序应提升包含具体技术组件名称的 `core_insight` 的权重。（待继续验证）
- 用户对 `source_type` 为 `paper`（特别是 arxiv）且包含 `evidence_snippet` 指出当前技术局限的内容表现出积极倾向。后续推送可适当增加此类高技术密度源的比例。（待继续验证）

### 喜欢的推送实例增量
- Enhancing Temporal Understanding in Video-LLMs through Stacked Temporal Attention in Vision Encoders | 来源: arxiv | 匹配原因: 该研究直接针对视频理解中的时间动态问题，与项目目标“以长视频深度理解为切入点”高度一致，提出的堆叠时间注意力方法可以显著提升模型在动作序列理解和时间进展方面的能力 | 链接: http://arxiv.org/abs/2510.26027v1

### 否定的推送实例增量
- 暂无新增。

### 本次反馈原因分析
- 本次标记为“有用”，核心触发因素是 `relevance_reason` 准确捕捉了项目“多模态技术打磨”中关于“长视频深度理解”的切入点，且 `core_insight` 给出了具体的“堆叠时间注意力模块”作为解决方案，直接回应了当前 Video-LLM 在时间动态上的局限。
