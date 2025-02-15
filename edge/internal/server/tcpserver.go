package server

import (
	"github.com/zhoushuguang/zeroim/common/discovery"
	"github.com/zhoushuguang/zeroim/common/socket"
	"github.com/zhoushuguang/zeroim/edge/client"
	"github.com/zhoushuguang/zeroim/edge/internal/svc"

	"github.com/zeromicro/go-zero/core/logx"
)

type TCPServer struct {
	svcCtx *svc.ServiceContext
	Server *socket.Server
}

func NewTCPServer(svcCtx *svc.ServiceContext) *TCPServer {
	return &TCPServer{svcCtx: svcCtx}
}

func (srv *TCPServer) HandleRequest() {
	for {
		// 里面逻辑为 conn, err := s.Listener.Accept()
		// Accept 到 conn 后，创建一个 session，并把 conn 封装到 session 中，返回session去读取和发送数据
		session, err := srv.Server.Accept()
		if err != nil {
			panic(err)
		}

		// 创建一个 client 对象，client 内置 IMRpc 对象，用于调用 IM 服务的 rpc 接口
		cli := client.NewClient(srv.Server.Manager, session, srv.svcCtx.IMRpc)
		go srv.sessionLoop(cli)
	}
}

func (srv *TCPServer) sessionLoop(client *client.Client) {
	message, err := client.Receive() // 实际是调用 Session.Receive() 读取数据
	if err != nil {
		logx.Errorf("[sessionLoop] client.Receive error: %v", err)
		_ = client.Close()
		return
	}

	// 登录校验，调用 IM 服务的 rpc 接口
	err = client.Login(message)
	if err != nil {
		logx.Errorf("[sessionLoop] client.Login error: %v", err)
		_ = client.Close()
		return
	}

	// 定时给客户端发送心跳包，用于保活 长连接
	go client.HeartBeat()

	for {
		message, err := client.Receive() // 不断从 tcp 链接读取数据
		if err != nil {
			logx.Errorf("[sessionLoop] client.Receive error: %v", err)
			_ = client.Close()
			return
		}
		err = client.HandlePackage(message) // 将读取到的数据交给 imRpc 处理
		if err != nil {
			logx.Errorf("[sessionLoop] client.HandleMessage error: %v", err)
		}
	}
}

func (srv *TCPServer) KqHeart() {
	work := discovery.NewQueueWorker(srv.svcCtx.Config.Etcd.Key, srv.svcCtx.Config.Etcd.Hosts, srv.svcCtx.Config.KqConf)
	work.HeartBeat()
}
