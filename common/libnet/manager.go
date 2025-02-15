package libnet

import (
	"sync"

	"github.com/zhoushuguang/zeroim/common/hash"
	"github.com/zhoushuguang/zeroim/common/session"
)

const sessionMapNum = 32

type Manager struct {
	Name        string
	sessionMaps [sessionMapNum]sessionMap // 数组大小为32，每个元素都是一个sessionMap // 分片管理session，减少锁竞争
	disposeFlag bool
	disposeOnce sync.Once
	disposeWait sync.WaitGroup
}

type sessionMap struct {
	sync.RWMutex
	sessions      map[session.Session]*Session // sessionStr -> session
	tokenSessions map[string][]session.Session // token -> sessionStr...
}

func NewManager(name string) *Manager {
	manager := &Manager{
		Name: name,
	}
	for i := 0; i < sessionMapNum; i++ {
		manager.sessionMaps[i].sessions = make(map[session.Session]*Session)
		manager.sessionMaps[i].tokenSessions = make(map[string][]session.Session)
	}

	return manager
}

func (m *Manager) GetSession(sessionId session.Session) *Session {
	token := sessionId.Token()
	hashId := hash.Hash([]byte(token))
	smap := &m.sessionMaps[hashId%sessionMapNum]

	smap.RLock()
	defer smap.RUnlock()

	return smap.sessions[sessionId]
}

func (m *Manager) GetTokenSessions(token string) []*Session {
	hashId := hash.Hash([]byte(token))
	smap := &m.sessionMaps[hashId%sessionMapNum]

	smap.RLock()
	defer smap.RUnlock()

	sessionIds := smap.tokenSessions[token]

	var sessions []*Session
	for _, sessionId := range sessionIds {
		sessions = append(sessions, smap.sessions[sessionId])
	}

	return sessions
}

func (m *Manager) AddSession(session *Session) {
	sessionId := session.Session()
	token := session.token
	hashId := hash.Hash([]byte(token))
	smap := &m.sessionMaps[hashId%sessionMapNum]

	smap.Lock()
	defer smap.Unlock()

	smap.sessions[sessionId] = session
	smap.tokenSessions[token] = append(smap.tokenSessions[token], sessionId)
}

func (m *Manager) removeSession(session *Session) {
}

func (m *Manager) Close() {
	m.disposeOnce.Do(func() {
		m.disposeFlag = true

		for i := 0; i < sessionMapNum; i++ {
			smap := &m.sessionMaps[i]
			smap.Lock()
			for _, session := range smap.sessions {
				session.Close()
			}
			smap.Unlock()
		}
		m.disposeWait.Wait()
	})
}
