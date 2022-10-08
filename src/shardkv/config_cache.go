package shardkv

import (
	"log"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

type configCache struct {
	mu      sync.RWMutex
	log     *log.Logger
	scclerk *shardctrler.Clerk
	configs map[int]shardctrler.Config
	nextNum int
}

func newConfigCache(log *log.Logger, ctrlers []*labrpc.ClientEnd) *configCache {
	cc := new(configCache)

	cc.log = raft.ExtendLoggerWithTopic(log, raft.LoggerTopic("CCCHE"))
	cc.scclerk = shardctrler.MakeClerk(ctrlers)
	cc.configs = make(map[int]shardctrler.Config)

	go cc.refresh()

	return cc

}

func (cc *configCache) get(cn int) shardctrler.Config {
	cc.mu.RLock()
	if cfg, ok := cc.configs[cn]; ok {
		cc.mu.RUnlock()
		return cfg
	}
	cc.mu.RUnlock()

	cfg := cc.scclerk.Query(cn)

	cc.set(cfg)

	return cfg
}

func (cc *configCache) set(cfg shardctrler.Config) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.log.Printf("Cache config CN:%d SHs:%v", cfg.Num, cfg.Shards)
	cc.configs[cfg.Num] = cfg
}

func (cc *configCache) refresh() {
	ticker := time.NewTicker(refreshConfigPeriod)

	for ; true; <-ticker.C {
		cc.mu.RLock()
		if _, ok := cc.configs[cc.nextNum]; ok {
			cc.nextNum++
			cc.mu.RUnlock()
			continue
		}
		cc.mu.RUnlock()

		cfg := cc.scclerk.Query(cc.nextNum)

		cc.mu.RLock()
		if _, ok := cc.configs[cfg.Num]; ok {
			cc.mu.RUnlock()
			continue
		}
		cc.mu.RUnlock()

		cc.set(cfg)

		cc.nextNum = cfg.Num + 1
	}
}
