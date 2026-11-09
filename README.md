# FastDD

多线程读取或原地重写文件，支持 SQLite 文件级断点续跑、实时彩色进度、吞吐量、ETA 和后台监控。运行时仅使用 Python 标准库，支持 Python 3.9+，Linux / macOS。

[PyPI 安装包](https://pypi.org/project/fastdd/) · [GitHub 源码](https://github.com/aristoteleo/fastdd) · [问题反馈](https://github.com/aristoteleo/fastdd/issues)

## 实际运行效果

![FastDD 实际运行输出：4 个文件重写、2 个按 mtime 跳过，显示清理日期与剩余天数](docs/screenshots/retention-demo.jpg)

截图来自 macOS 上 FastDD 1.2.0 的真实运行：6 个临时文件共 48 MiB，其中 4 个文件按 90 天清理周期计算已剩余不超过 30 天，因此重写；另外 2 个文件剩余超过 30 天，因此跳过。处理后全部 6 个文件的 SHA-256 校验通过，内容保持一致。速度仅反映这次本地演示环境。

截图采用真实 PTY 终端输出在浏览器终端中回放后截取的方式，保留原始数值和 ANSI 配色。原始记录见 [retention-demo.ansi](docs/screenshots/retention-demo.ansi)。

安装 FastDD 并获取本仓库后，可在仓库目录复现：

```bash
python examples/retention_demo.py
```

演示仅创建临时测试文件，验证完毕后自动删除；不会处理你的现有数据。

## 安装

从 PyPI 安装：

```bash
pip install fastdd
fastdd --version
fastdd --help
```

也可以使用独立环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastdd
```

安装后所有操作均使用 `fastdd`。命令所在的 Python 环境需已激活或在 PATH 中。也可以下载源码后在项目目录运行 `pip install .`，或通过 `pip install /path/to/fastdd-1.2.0-py3-none-any.whl` 安装 wheel。

## 使用

读取目录内的所有普通文件：

```bash
fastdd run /data/dataset --mode read --workers 8 --state /tmp/fastdd-dataset
```

原地读出并写回相同内容：

```bash
fastdd run '/data/batch*' --mode rewrite --workers 8 --chunk-mb 32 --fsync --state /tmp/fastdd-rewrite
```

`rewrite` 会实际写入文件，改变修改时间；请用于处理期间不会被其他程序写入的文件。`--fsync` 在每个文件完成时同步写入。它不是备份或原子替换。

后台运行与监控（日志放在输入目录之外）：

```bash
nohup fastdd run /data/dataset --workers 8 --state /tmp/fastdd-dataset > /tmp/fastdd-dataset.log 2>&1 &
fastdd watch --state /tmp/fastdd-dataset
fastdd watch --state /tmp/fastdd-dataset --once
```

彩色输出：

```bash
fastdd run /data/dataset --state /tmp/fastdd-dataset --color always
fastdd watch --state /tmp/fastdd-dataset --color never
```

默认 `--color auto`：交互终端有颜色，重定向日志无 ANSI 颜色。支持 `NO_COLOR` 环境变量；显式 `--color always` 优先。`--color` 可放在子命令前或后。绿色表示完成、黄色表示扫描/跳过、红色表示错误、青色表示吞吐量与进度。

## 按修改时间筛选（1.2.0 新增）

如果服务器按照最后修改时间（mtime）满 90 天清理文件，可以只处理剩余不超过 30 天的文件：

```bash
fastdd run /data/dataset \
  --mode rewrite \
  --retention-days 90 \
  --skip-if-remaining-days 30 \
  --workers 16 \
  --state /tmp/fastdd-dataset
```

计算方式：`预计清理日期 = mtime + 90 天`，`剩余天数 = (预计清理日期 - 当前时间) / 86400`。

| 文件距上次修改 | 剩余时间 | 行为 |
| --- | --- | --- |
| 20 天 | 70 天 | 跳过 |
| 59 天 | 31 天 | 跳过 |
| 60 天 | 30 天 | 处理 |
| 75 天 | 15 天 | 处理 |
| 95 天 | 已超过预计清理时间 5 天 | 处理 |

严格按秒比较，**剩余 > 30 天跳过，≤ 30 天处理**，不先四舍五入。`--retention-days` 默认 90；只有指定 `--skip-if-remaining-days` 才会开启筛选。处理前再次检查 mtime，避免对排队期间已经被更新的文件重复处理。

面板显示任务开始/更新时间；开启筛选时，还显示当前文件及最近文件的 mtime、预计清理日期、剩余天数，以及因时间充足而跳过的文件数。日期使用运行命令所在机器的本地时区。活跃文件显示处理前时间，`DONE` 显示处理完成后的时间，`SKIPPED` 显示判断时的时间；历史记录的剩余天数不会持续倒计时。

此模式根据当前 mtime 决定是否处理，**不会因为旧的完成记录而跳过已经再次临近清理的文件**。`--no-resume` 也不会取消时间筛选；要处理全部文件，请省略 `--skip-if-remaining-days` 并使用 `--no-resume`。

`rewrite` 写回内容并更新 mtime；开启筛选时，空文件通过更新时间戳处理。`read` 不更新 mtime，因此不会延后基于 mtime 的清理日期，程序会显示提示。清理日期是按你提供的周期推算，实际清理时间仍取决于服务器规则和清理任务的执行时间。

## 断点与状态

- 未开启时间筛选时，重复相同命令和 `--state` 会跳过路径、大小、修改时间与记录一致的已完成文件。
- `read` 和 `rewrite` 分别记录，切换模式不会误跳过。旧版脚本的数据库可以复用路径，但首次升级会重新处理，因为旧记录没有模式信息。
- 使用 `--no-resume` 强制重新处理，例如再次预热可能已经被系统淘汰的缓存。已完成记录不代表数据仍在缓存中。
- Ctrl+C / SIGTERM 在当前 I/O 调用完成后停止；未完成的文件下次从头处理。阻塞中的系统 I/O 可能需要等待。
- 状态保存为 `<state>.sqlite`、`<state>.status.json` 和 `<state>.lock`；当前任务自身的状态文件自动排除。一个状态前缀同时只允许一个任务使用。
- 跳过符号链接，不跟随符号链接目录。扫描进度在扫描结束后显示百分比；完成进度包括跳过和失败文件，是否成功应查看 `Failed` 和退出码。
- 吞吐量按处理的文件字节计算；`rewrite` 同时读写，显示值不是底层存储总 I/O 量。预热效果受操作系统和存储缓存策略影响。
- 默认每个 worker 使用最多 32 MiB 数据块，线程越多不保证越快。

退出码：`0` 成功，`1` 部分文件失败，`2` 参数/启动/状态错误，`130` 任务被中断。`watch` 成功显示状态时返回 `0`；查看失败数以判断被监控任务结果。

## 开发与验证

```bash
pip install -e .
python3 -m unittest discover -s tests -v
```

测试仅操作临时目录，覆盖读写内容、断点模式隔离、状态文件排除、颜色策略、短写处理、中断退出、时间阈值边界、过期文件、空文件及旧断点与时间筛选的交互。
