package impl

import (
	"analyzer/checker/config"
	"errors"
	"fmt"
	"maps"
	"net"
	"os/exec"
	"sync"
	"time"

	"github.com/op/go-logging"
)

type Monitor struct {
	con        config.Config
	log        *logging.Logger
	conn       *net.UDPConn
	watchNodes map[string]*net.UDPAddr
	wg         sync.WaitGroup
}

func newMonitor(con config.Config, watchNodeNames []string) (Monitor, error) {
	watchNodes := make(map[string]*net.UDPAddr, len(watchNodeNames))
	for _, nodeName := range watchNodeNames {
		addrStr := fmt.Sprintf("%s:%d", nodeName, con.HealthCheckPort)
		addr, err := net.ResolveUDPAddr("udp", addrStr)
		if err != nil {
			return Monitor{}, fmt.Errorf("failed to resolve network address for node name %s: %v", nodeName, err)
		}
		watchNodes[nodeName] = addr
	}

	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return Monitor{}, fmt.Errorf("failed to open udp connection for keep alive: %v", err)
	}

	return Monitor{
		con:        con,
		conn:       conn,
		watchNodes: watchNodes,
	}, nil
}

func SpawnMonitor(con config.Config, log *logging.Logger, watchNodes []string) (*Monitor, error) {
	m, err := newMonitor(con, watchNodes)
	if err != nil {
		return nil, err
	}

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.run()
		log.Infof("Terminating Monitor...")
	}()

	return &m, nil
}

func (m *Monitor) direct(addr *net.UDPAddr) bool {
	_, err := m.conn.WriteToUDP(nil, addr)
	return err == nil
}

func (m *Monitor) broadcast(addrs map[string]*net.UDPAddr) bool {
	for _, addr := range addrs {
		if !m.direct(addr) {
			return false
		}
	}
	return true
}

func (m *Monitor) wait(dur time.Duration, dead map[string]*net.UDPAddr) (bool, error) {
	now := time.Now()
	deadline := now.Add(dur)

	if err := m.conn.SetReadDeadline(deadline); err != nil {
		return false, fmt.Errorf("failed to set deadline timeout: %v", err)
	}

	for {
		_, peerAddr, err := m.conn.ReadFromUDP(nil)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return true, nil
			}

			if netErr, ok := err.(net.Error); ok && errors.Is(netErr, net.ErrClosed) {
				return false, nil
			}

			return false, fmt.Errorf("an error occurred while waiting for acks: %v", err)
		}

		name := peerAddr.IP.String()
		delete(dead, name)
	}
}

func (m *Monitor) revive(containerName string) error {
	m.log.Infof("Killing %s", containerName)
	cmd := exec.Command("docker", "kill", "--signal=9", containerName)
	cmd.Run()

	cmd = exec.Command("docker", "compose", "-f", m.con.CheckerComposePath, "up", "-d", containerName)

	m.log.Infof("Trying to revive %s", containerName)
	var err error
	for range 1 + m.con.ReviveRetries {
		if err = cmd.Run(); err == nil {
			break
		}
	}

	if err != nil {
		return fmt.Errorf("failed to revive %s: %v", containerName, err)
	}

	m.log.Infof("Successfuly revived %s", containerName)
	return nil
}

func (m *Monitor) run() {
	dead := make(map[string]*net.UDPAddr, len(m.watchNodes))

	for {
		maps.Copy(dead, m.watchNodes)

		waitDur := m.con.StartingKeepAliveWaitDuration
		for range 1 + m.con.KeepAliveRetries {
			m.log.Debugf("Broadcasting keep-alives to %d nodes", len(dead))
			if !m.broadcast(dead) {
				return
			}

			m.log.Debugf("Waiting for ACKs...")
			ok, err := m.wait(waitDur, dead)
			if err != nil {
				m.log.Error(err)
			}
			if !ok {
				return
			}

			if len(dead) == 0 {
				break
			}

			waitDur <<= 1
		}

		sleepDur := m.con.DefaultSleepDuration
		m.log.Infof("Detected %d dead nodes", len(dead))
		for name := range dead {
			if err := m.revive(name); err != nil {
				m.log.Error(err)
				continue
			}

			delete(dead, name)
			sleepDur = m.con.ReviveSleepDuration
		}

		m.log.Infof("Sleeping for %d seconds...", sleepDur)
		time.Sleep(sleepDur)
	}
}

func (m *Monitor) Stop() error {
	m.log.Debugf("Waiting for Monitor to stop...")

	err := m.conn.Close()
	m.wg.Wait()

	m.log.Debugf("Monitor stoped")
	return err
}
