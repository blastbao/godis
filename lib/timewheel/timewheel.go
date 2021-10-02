package timewheel

import (
	"container/list"
	"github.com/hdt3213/godis/lib/logger"
	"time"
)

// TimeWheel can execute job after waiting given duration
type TimeWheel struct {

	interval time.Duration	// tick 间隔
	ticker   *time.Ticker	// tick 定时器
	slots    []*list.List	// 时间轮

	timer             map[string]int // 任务名 => slot index
	currentPos        int
	slotNum           int
	addTaskChannel    chan task
	removeTaskChannel chan string
	stopChannel       chan bool
}

type task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
}

// New creates a new time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval:          interval,
		slots:             make([]*list.List, slotNum),
		timer:             make(map[string]int),
		currentPos:        0,
		slotNum:           slotNum,
		addTaskChannel:    make(chan task),
		removeTaskChannel: make(chan string),
		stopChannel:       make(chan bool),
	}
	tw.initSlots()

	return tw
}


// 初始化 n 个 slots
func (tw *TimeWheel) initSlots() {
	for i := 0; i < tw.slotNum; i++ {
		tw.slots[i] = list.New()
	}
}

// Start starts ticker for time wheel
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

// Stop stops the time wheel
func (tw *TimeWheel) Stop() {
	tw.stopChannel <- true
}

// AddJob add new job into pending queue
func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.addTaskChannel <- task{
		delay: delay,
		key: key,
		job: job,
	}
}

// RemoveJob add remove job from pending queue
// if job is done or not found, then nothing happened
func (tw *TimeWheel) RemoveJob(key string) {
	if key == "" {
		return
	}
	tw.removeTaskChannel <- key
}

func (tw *TimeWheel) start() {
	for {
		select {
		// 定时
		case <-tw.ticker.C:
			tw.tickHandler()
		// 新增任务
		case task := <-tw.addTaskChannel:
			tw.addTask(&task)
		// 删除任务
		case key := <-tw.removeTaskChannel:
			tw.removeTask(key)
		// 退出
		case <-tw.stopChannel:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	// 当前槽
	l := tw.slots[tw.currentPos]

	// 遍历当前槽的任务链表
	tw.scanAndRunTask(l)

	// 递增槽
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0 // 回绕
	} else {
		tw.currentPos++
	}
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	// 遍历任务链表
	for e := l.Front(); e != nil; {
		// 当前任务
		task := e.Value.(*task)

		//
		if task.circle > 0 {
			task.circle-- // 轮次减 1 ，这样在下一次检查时再判断是否应该执行
			e = e.Next()
			continue
		}

		// 启动任务
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			job := task.job
			job()
		}()

		// 移除当前任务
		next := e.Next()
		l.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

func (tw *TimeWheel) addTask(task *task) {
	pos, circle := tw.getPositionAndCircle(task.delay)
	task.circle = circle

	tw.slots[pos].PushBack(task)

	if task.key != "" {
		tw.timer[task.key] = pos
	}
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	// 在 xxx 秒之后执行
	delaySeconds := int(d.Seconds())

	// 定时 tick 间隔
	intervalSeconds := int(tw.interval.Seconds())

	// 需要多少 tick: ticks := delaySeconds / intervalSeconds
	// 需要多少轮: circles := ticks / slotNum

	circle = delaySeconds / intervalSeconds / tw.slotNum
	pos = (tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum
	return
}

func (tw *TimeWheel) removeTask(key string) {
	// 根据任务名 key 查询 slot index
	idx, ok := tw.timer[key]
	if !ok {
		return
	}

	// 取 slot
	l := tw.slots[idx]

	// 将 key 任务从 list 中移除
	for e := l.Front(); e != nil; {
		task := e.Value.(*task)
		if task.key == key {
			delete(tw.timer, task.key)
			l.Remove(e)
		}
		e = e.Next()
	}
}
