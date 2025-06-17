package impl

import (
	"analyzer/checker/config"
	"errors"
	"fmt"
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
	watchNodes []string
	wg         sync.WaitGroup
}

func (m *Monitor) resolveHost(nodeName string) (*net.UDPAddr, error) {
	addrStr := fmt.Sprintf("%s:%d", nodeName, m.con.HealthCheckPort)
	addr, err := net.ResolveUDPAddr("udp", addrStr)
	if err != nil {
		return nil, err
	}

	return addr, nil
}

func newMonitor(con config.Config, log *logging.Logger, watchNodeNames []string) (Monitor, error) {
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return Monitor{}, fmt.Errorf("failed to open udp connection for keep alive: %v", err)
	}

	return Monitor{
		con:        con,
		log:        log,
		conn:       conn,
		watchNodes: watchNodeNames,
	}, nil
}

func SpawnMonitor(con config.Config, log *logging.Logger, watchNodes []string) (*Monitor, error) {
	m, err := newMonitor(con, log, watchNodes)
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

func (m *Monitor) direct(name string) bool {
	addr, err := m.resolveHost(name)
	if err != nil {
		return true
	}
	_, err = m.conn.WriteToUDP(nil, addr)
	return err == nil
}

func (m *Monitor) broadcast(nodes map[string]string) bool {
	allOk := true
	for _, name := range nodes {
		if !m.direct(name) {
			allOk = false
		}
	}
	return allOk
}

func (m *Monitor) wait(dur time.Duration, dead map[string]string) (bool, error) {
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

		m.log.Debugf("Received ACK from %s", peerAddr.IP.String())
		delete(dead, peerAddr.IP.String())
	}
}

func (m *Monitor) revive(containerName string) error {
	m.log.Infof("Trying to revive %s", containerName)

	var err error
	for range 1 + m.con.ReviveRetries {
		cmd := exec.Command("docker", "restart", containerName)
		if err = cmd.Run(); err == nil {
			break
		}
	}

	if err != nil {
		return fmt.Errorf("failed to revive %s: %v", containerName, err)
	}

	m.log.Infof("Successfully revived %s", containerName)
	return nil
}

func (m *Monitor) run() {
	dead := make(map[string]string, len(m.watchNodes))
	failedToResolve := make(map[string]struct{})

	for {
		for _, name := range m.watchNodes {
			if addr, err := m.resolveHost(name); err != nil {
				failedToResolve[name] = struct{}{}
			} else {
				dead[addr.IP.String()] = name
			}
		}

		waitDur := m.con.StartingKeepAliveWaitDuration
		for range 1 + m.con.KeepAliveRetries {
			m.log.Debugf("Broadcasting keep-alives to %d nodes", len(dead))
			if !m.broadcast(dead) {
				return
			}

			m.log.Debugf("Waiting for ACKs for %d seconds...", int(waitDur.Seconds()))
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

			m.log.Infof("%d didn't respond", len(dead))
			waitDur <<= 1
		}

		sleepDur := m.con.DefaultSleepDuration
		m.log.Infof("Dead nodes: %d", len(dead))
		for _, name := range dead {
			if err := m.revive(name); err != nil {
				m.log.Error(err)
				continue
			}

			delete(dead, name)
			sleepDur = m.con.ReviveSleepDuration
		}

		m.log.Infof("Unresolved address: %d", len(failedToResolve))
		for name := range failedToResolve {
			if err := m.revive(name); err != nil {
				m.log.Error(err)
			}

			delete(failedToResolve, name)
			sleepDur = m.con.ReviveSleepDuration
		}

		m.log.Infof("Sleeping for %d seconds...", int(sleepDur.Seconds()))
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
