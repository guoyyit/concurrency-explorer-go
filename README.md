# concurrency-explorer-go

`sct`（systematic concurrency testing）是一个本地可复现的并发调度探索与缺陷检测库。
被测程序用**受控并发原语**编写，工具在有界预算内系统枚举线程调度，
对数据竞争、断言违反、运行异常、死锁与非终止等待给出**可精确重放**的结论。

## 快速开始

```go
package example

import (
	"testing"

	"github.com/guoyyit/concurrency-explorer-go/sct"
)

func TestRace(t *testing.T) {
	program := func(e *sct.Env) {
		x := sct.NewVar[int](e, "x")
		e.Go(func(c *sct.Env) { x.Set(c, 1) })
		e.Go(func(c *sct.Env) { x.Set(c, 2) })
	}
	rep := sct.Explore(sct.Config{Seed: 1}, program)
	t.Log(rep.String()) // 报告竞争：位置、读写类型、触发调度

	// 确定性重放：同一调度必然得到同一结论
	witness := rep.Races[0]
	res := sct.Replay(program, witness.Schedule)
	_ = res // 与探索时完全一致的事件序列与结论
}
```

## 受控原语

被测程序只能使用本包提供的原语；每个原语调用都是一个**调度点**：

| 原语 | 说明 |
| --- | --- |
| `e.Go(func(*sct.Env))` | 创建受控线程 |
| `sct.NewMutex / Lock / Unlock` | 互斥锁（不可重入，误解锁记为 panic） |
| `sct.NewChan[T] / Send / Recv / Close` | 通道，容量 0 为会合语义 |
| `sct.NewWaitGroup / Add / Done / Wait` | 等待组 |
| `sct.NewVar[T] / Get / Set` | 受跟踪的内存单元（竞争检测对象） |
| `sct.Assert(e, cond, msg)` | 断言，失败即记录并终止本次运行 |
| `sct.Yield(e)` | 纯调度点 |

## 调度模型

- 任意时刻只有一个受控线程在运行。线程在每个受控操作上向调度器登记并阻塞，
  调度器从**已就绪**线程集合中选择一个放行。
- 一次运行完全由 **schedule**（每步选择的线程序列）决定。
- `Explore` 以无状态方式枚举调度：每次运行从头重放前缀，再对未选择的分支生成新前缀
  （深度优先、种子控制的随机次序）。**相同 Seed + 相同程序 ⇒ 完全相同的结论与调度序列。**
- `Replay` 严格按给定 schedule 强制选择，不引入任何新交错；
  程序若偏离 schedule（选择不可用、执行长于 schedule）返回 `VerdictError`。

## 缺陷判定

- **数据竞争**：基于向量时钟的 happens-before 分析。两个对同一 `Var` 的访问
  （至少一个写）在某个已探索运行中无序时报告，报告含位置、读写类型、双方调用点与见证调度。
  互斥锁、通道、WaitGroup、spawn 建立的 happens-before 边均被跟踪，
  因此**始终有序的访问不会误报**；相对地，未探索到的调度不在结论范围内。
- **断言违反 / 运行异常（panic）**：记录失败调度，随报告给出可复现 witness。
  无法触发的断言在完整探索后会得到 `no defects found in explored schedules`
  及已探索调度数的说明。
- **死锁**：所有存活线程均永久阻塞时判定。报告给出等待图：每个参与线程、
  所依赖资源（mutex/chan/wg）、互斥锁持有方，以及线程间等待环（如 `T2 -> T3 -> T2`）。
- **非终止等待**：单运行步数超过 `MaxSteps` 判定为 non-terminating（可能是活锁或无界计算），
  与死锁严格区分；暂时阻塞后被释放属于正常终止，不会混淆。

## 探索预算

`Config` 提供四类预算，超出时报告标记 `INCOMPLETE` 并说明未覆盖范围
（已探索调度数、已知但未探索的前缀数）：

- `MaxSchedules`：枚举的调度总数上限（0 = 不限）。
- `MaxSteps`：单次运行的步数上限（默认 10000），是非终止的**确定性**判据。
- `Timeout`：整体墙钟超时，是安全阀；由它触发的停止时机不具备跨机器确定性。
- `MaxThreads`：线程总数上限（默认 64），超限记为 `VerdictError`。

报告在探索循环结束后由**已完整结束的运行**一次性汇总；
中断或预算耗尽不会留下相互矛盾的部分结论，只会将探索标记为不完整。

## 结论的适用边界

1. **只有受控操作是调度点。** 真实的 `sync.Mutex`、原生 channel、未跟踪的普通变量
   对工具不可见；被测程序必须全部改用受控原语。
2. **程序需是调度确定的。** 在相同调度下，线程执行的操作序列必须唯一
   （不得依赖墙钟时间、真随机数、map 迭代序等）。否则探索会报
   `schedule diverged`（`VerdictError`）。
3. **无 DPOR。** 枚举是朴素的系统性前缀展开，调度空间随线程数与步数指数增长；
   大程序必须配合预算使用，此时结论只覆盖已探索范围（报告会明确说明）。
4. **纯计算死循环无法抢占。** 不含任何受控操作的无限循环无法被步数预算中断
   （调度器在等待该线程登记），会触发收集超时并记为 non-terminating。
5. **竞争检测对单运行精确。** happens-before 判定本身无误报；
   但“无竞争”结论仅在探索完整（`Complete == true`）时覆盖全部调度。

## 测试

```sh
go test ./...
```

覆盖：数据竞争、互斥/通道序下的无误报、断言失败与不可达断言、panic 定位、
互斥锁死锁环、通道死锁、暂时阻塞的正常终止、非终止（活锁）、确定性重放、
同种子确定性、调度/步数/线程/超时预算耗尽。
