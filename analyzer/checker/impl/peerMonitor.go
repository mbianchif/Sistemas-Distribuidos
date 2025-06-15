package impl

import (
	"analyzer/checker/config"
	"fmt"
	"net"
	"os/exec"
	"sync"
	"time"

	"github.com/op/go-logging"
)

type PeerMonitor struct {
	con      config.Config
	conn     *net.UDPConn
	log      *logging.Logger
	nextAddr *net.UDPAddr
	wg       sync.WaitGroup
	exit     bool
}

func newPeerMonitor(con config.Config, log *logging.Logger) (PeerMonitor, error) {
	addr, err := net.ResolveUDPAddr("udp", "0.0.0.0")
	if err != nil {
		return PeerMonitor{}, fmt.Errorf("failed to resolve network address for keep alive: %v", err)
	}

	nextId := (con.Id + 1) % con.N
	host := fmt.Sprintf(con.HostFmt, nextId)
	addrStr := fmt.Sprintf("%s:%d", host, con.HealthCheckPort)
	nextAddr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return PeerMonitor{}, err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return PeerMonitor{}, fmt.Errorf("failed to open udp connection for keep alive: %v", err)
	}

	return PeerMonitor{
		conn:     conn,
		con:      con,
		log:      log,
		nextAddr: nextAddr,
	}, nil
}

func SpawnPeerMonitor(con config.Config, log *logging.Logger) error {
	m, err := newPeerMonitor(con, log)
	if err != nil {
		return err
	}
	return m.run()
}

func (m *PeerMonitor) sendNext() error {
	_, err := m.conn.WriteToUDP([]byte{}, m.nextAddr)
	return err
}

func (m *PeerMonitor) waitNext(dur time.Duration) bool {
	now := time.Now()
	deadline := now.Add(dur)

	if err := m.conn.SetReadDeadline(deadline); err != nil {
		return false
	}

	for {
		_, peerAddr, err := m.conn.ReadFromUDP([]byte{})
		if err != nil {
			return false
		}

		if peerAddr == m.nextAddr {
			return true
		}
	}
}

func (m *PeerMonitor) run() error {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		for !m.exit {
			if err := m.sendNext(); err != nil {
				break
			}

			retries := 0
			waitDur := m.con.StartingKeepAliveWaitDuration
			for ; retries <= m.con.KeepAliveRetries; retries++ {
				if m.waitNext(waitDur) {
					break
				}

				waitDur <<= 1
			}

			if m.exit {
				break
			}

			sleepDur := m.con.DefaultSleepDuration
			if retries > m.con.KeepAliveRetries {
				m.respawnNext(string(m.nextAddr.IP))
				sleepDur = m.con.RespawnSleepDuration
			}

			time.Sleep(sleepDur)
		}
	}()

	return nil
}

func (m *PeerMonitor) respawnNext(containerName string) error {
	cmd := exec.Command("docker", "kill", "--signal=9", containerName)
	cmd.Run()

	cmd = exec.Command("docker", "compose", "-f", m.con.CheckerComposeFileName, "up", "-d", containerName)
	err := cmd.Run()

	for i := 0; err != nil && i < m.con.RespawnRetries; i++ {
		err = cmd.Run()
	}

	if err != nil {
		return fmt.Errorf("failed to respawn %s: %v", containerName, err)
	}

	return nil
}

func (m *PeerMonitor) Stop() error {
	m.exit = true
	err := m.conn.Close()
	m.wg.Wait()
	m.log.Infof("Terminating Peer Monitor...")
	return err
}
