# SMART Sleeper Dynamic Filtering Update

本目录按照共享 GitHub 项目的仓库根目录组织，可直接对照原路径上传或合并。

## 上传范围

### 动态过滤核心

- `filters/model_rules.py`
- `filters/engine.py`
- `filters/state.py`
- `filters/models.py`
- `filters/config.py`
- `config/dynamic_filtering.json`

### 动态过滤基础模块

- `filters/event_rules.py`
- `filters/adaptive_rules.py`
- `filters/context_rules.py`

如果共享仓库已有相同版本，可以保留仓库版本；否则应一起上传。

### 模型、训练与依赖

- `train_autoencoder.py`
- `models/autoencoder-v0/metadata.json`
- `models/autoencoder-v0/weights.npz`
- `requirements.txt`

### 测试与说明

- `tests/test_model_rules.py`
- `tests/test_train_autoencoder.py`
- `docs/autoencoder.md`

### 命令行接入文件

- `run_pipeline.py`
- `filter_rules.py`

只有共享仓库尚未支持 `--dynamic-mode auto` 或缺少动态状态、模型报告接入时，才合并这两个文件中的动态过滤相关改动。必须保留共享仓库原有的数据入口、定时方式、Supabase 上传和输出路径。

## 明确未包含

- 原始 CSV/JSON 数据
- `.env` 和任何凭据
- 虚拟环境、缓存与临时文件
- 定时任务脚本
- 本地动态状态和过滤输出
- 与动态过滤无关的 Supabase、解码器或数据入口文件

## 建议上传顺序

1. `filters/` 与 `config/`
2. `train_autoencoder.py`、`models/` 和 `requirements.txt`
3. `tests/` 与 `docs/`
4. 最后根据共享仓库实际情况，选择性合并 `run_pipeline.py` 和 `filter_rules.py`
