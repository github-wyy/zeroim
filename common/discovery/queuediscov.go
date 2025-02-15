package discovery

import (
	"github.com/zeromicro/go-queue/kq"
	"github.com/zeromicro/go-zero/core/discov"
)

// 这里的作用是为了在 kafaka 配置信息发生增删改时，感知到变化
type QueueObserver interface {
	Update(string, kq.KqConf)
	Delete(string)
}

func QueueDiscoveryProc(conf discov.EtcdConf, qo QueueObserver) {
	master, err := NewQueueMaster(conf.Key, conf.Hosts)
	if err != nil {
		panic(err)
	}

	// 将 queuelist 即 qo 赋值给 master 的 members 字段
	master.Register(qo)
	// 监听 etcd 中 key 前缀 为 edge 的 key-value 变化，并将变化赋值给 master 的 members 字段
	master.WatchQueueWorkers()
}
