package libnet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

const maxBodySize = 1 << 12 // 4KB

/*
总长度
----|
header头长度
--|
header头
-|-|--|--|----|------...
版本号|状态码|消息类型|命令|seq|pb body体

header头长度=1字节版本号+1字节状态码+2字节消息类型+2字节命令+4字节seq
总长度=header头+header头长度+pb body体长度

----|--|-|-|--|--|----|body
总长度|header头长度|版本号|状态码|消息类型|命令|seq｜body
总长度=2+1+1+2+2+4+len(body)
header头长度=1+1+2+2+4
*/

/*
connection：
----|--|-|-|--|--|----|body

message：
--|-|-|--|--|----|body
*/

const (
	packSize      = 4
	headerSize    = 2 // 占用2个字节用来表示header头的长度
	verSize       = 1
	statusSize    = 1
	serviceIdSize = 2
	cmdSize       = 2
	seqSize       = 4

	// header内容占用的字节数
	rawHeaderSize = verSize + statusSize + serviceIdSize + cmdSize + seqSize
	// 最大包长度 = 最大body长度(4KB) + header(10Bytes) + header头长度(2Bytes) + 总长度(4Bytes)
	maxPackSize = maxBodySize + rawHeaderSize + headerSize + packSize

	// offset
	headerOffset    = 0
	verOffset       = headerOffset + headerSize
	statusOffset    = verOffset + verSize
	serviceIdOffset = statusOffset + statusSize
	cmdOffset       = serviceIdOffset + serviceIdSize
	seqOffset       = cmdOffset + cmdSize
	bodyOffset      = seqOffset + seqSize
)

var (
	// codec： 编解码器，负责对消息进行编码和解码，比如pb、json、msgpack等
	ErrRawPackLen   = errors.New("default server codec pack length error")
	ErrRawHeaderLen = errors.New("default server codec header length error")
)

type Header struct {
	Version   uint8
	Status    uint8
	ServiceId uint16
	Cmd       uint16
	Seq       uint32
}

// --|-|-|--|--|----|body
type Message struct {
	Header
	Body []byte
}

func (m *Message) Fromat() string {
	return fmt.Sprintf("Version:%d, Status:%d, ServiceId:%d, Cmd:%d, Seq:%d, Body:%s",
		m.Version, m.Status, m.ServiceId, m.Cmd, m.Seq, string(m.Body))
}

type Protocol interface {
	NewCodec(conn net.Conn) Codec
}

type Codec interface {
	// 设置读取数据的超时时间，如果在设定时间内没有读取到数据，将返回超时错误，用于防止网络读取操作无限期阻塞
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
	Receive() (*Message, error)
	Send(Message) error
	Close() error
}

// 自定义协议
type IMProtocol struct{}

func NewIMProtocol() Protocol {
	return &IMProtocol{}
}

func (p *IMProtocol) NewCodec(conn net.Conn) Codec {
	return &imCodec{conn: conn}
}

// 自定义编解码器
type imCodec struct {
	conn net.Conn
}

func (c *imCodec) readPackSize() (uint32, error) {
	return c.readUint32BE()
}

// todo 有无其他更好的方式获取包的长度？
func (c *imCodec) readUint32BE() (uint32, error) {
	b := make([]byte, packSize)
	_, err := io.ReadFull(c.conn, b) // 消息的前4个字节存储了整个数据包的长度
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

/*
// io.ReadFull 用于精确从 r 中读取 len(buf) 个字节，返回读取的字节数和遇到的错误。如果长度和期望的长度不匹配，则返回 ErrUnexpectedEOF 错误。
// 所以不用下面这种，下面这种和 io.Read() 一样
func (c *imCodec) readUint32BE() (uint32, error) {
	b := make([]byte, packSize)
	n, err := io.ReadFull(c.conn, b)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return 0, fmt.Errorf("连接已关闭，未能读取完整数据: %w", err)
		} else if errors.Is(err, io.ErrUnexpectedEOF) {
			// 处理读取到的部分数据的情况
			return binary.BigEndian.Uint32(b[:n]), nil
		}

		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}
*/

func (c *imCodec) readPacket(msgSize uint32) ([]byte, error) {
	b := make([]byte, msgSize)
	_, err := io.ReadFull(c.conn, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (c *imCodec) Receive() (*Message, error) {
	packLen, err := c.readPackSize()
	if err != nil {
		return nil, err
	}

	if packLen > maxPackSize {
		return nil, ErrRawPackLen
	}

	buf, err := c.readPacket(packLen)
	if err != nil {
		return nil, err
	}

	var msg Message
	headerLen := binary.BigEndian.Uint16(buf[headerOffset:verOffset])
	msg.Version = buf[verOffset]
	msg.Status = buf[statusOffset]
	msg.ServiceId = binary.BigEndian.Uint16(buf[serviceIdOffset:cmdOffset])
	msg.Cmd = binary.BigEndian.Uint16(buf[cmdOffset:seqOffset])
	msg.Seq = binary.BigEndian.Uint32(buf[seqOffset:bodyOffset])
	//logx.Infof("msg.Seq:%+v", msg.Seq)

	if headerLen != rawHeaderSize {
		return nil, ErrRawHeaderLen
	}

	if packLen > uint32(headerLen) {
		msg.Body = buf[bodyOffset:packLen]
	}

	logx.Infof("receive msg:%+v", msg)
	return &msg, nil
}

func (c *imCodec) Send(msg Message) error {
	packLen := headerSize + rawHeaderSize + len(msg.Body)
	packLenBuf := make([]byte, packSize)
	binary.BigEndian.PutUint32(packLenBuf[:packSize], uint32(packLen))

	buf := make([]byte, packLen)
	// header
	binary.BigEndian.PutUint16(buf[headerOffset:], uint16(rawHeaderSize))
	buf[verOffset] = msg.Version
	buf[statusOffset] = msg.Status
	binary.BigEndian.PutUint16(buf[serviceIdOffset:], msg.ServiceId)
	binary.BigEndian.PutUint16(buf[cmdOffset:], msg.Cmd)
	binary.BigEndian.PutUint32(buf[seqOffset:], msg.Seq)

	// body
	// 安全性：copy()可以确保不会发生越界访问。它只会复制目标切片能容纳的数据量，避免了内存溢出的风险
	// 数据隔离：copy()会创建数据的副本，这样即使原始的msg.Body在其他地方被修改，也不会影响到要发送的数据
	// 正确的内存布局：这里需要将body数据放到预先分配好的buf切片的特定位置(headerSize+rawHeaderSize之后)，copy()能保证数据被正确放置
	copy(buf[headerSize+rawHeaderSize:], msg.Body)
	allBuf := append(packLenBuf, buf...)
	n, err := c.conn.Write(allBuf)
	if err != nil {
		return err
	}
	if n != len(allBuf) {
		return fmt.Errorf("n:%d, len(buf):%d", n, len(buf))
	}
	return nil
}

// 设置读超时
func (c *imCodec) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *imCodec) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *imCodec) Close() error {
	return c.conn.Close()
}
