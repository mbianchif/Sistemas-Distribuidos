package impl

import (
	"analyzer/checker/config"
	"errors"
	"fmt"
	"maps"
	"net"
	"os/exec"
	"slices"
	"sync"
	"time"

	"github.com/op/go-logging"
)

type Monitor struct {
	con        config.Config
	log        *logging.Logger
	conn       *net.UDPConn
	nameToAddr map[string]*net.UDPAddr
	addrToName map[string]string
	wg         sync.WaitGroup
}

func revive(containerName string, retries int, log *logging.Logger, healthCheckPort uint16) (*net.UDPAddr, error) {
	var err error
	for range 1 + retries {
		cmd := exec.Command("docker", "restart", containerName)
		if err = cmd.Run(); err == nil {
			break
		}
	}

	if err != nil {
		return nil, fmt.Errorf("failed to revive %s: %v", containerName, err)
	}

	log.Infof("Successfully revived %s", containerName)
	return resolveAddr(containerName, healthCheckPort)
}

func resolveAddr(host string, port uint16) (*net.UDPAddr, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	if addr, err := net.ResolveUDPAddr("udp", addr); err != nil {
		return nil, err
	} else {
		return addr, nil
	}
}

func newMonitor(con config.Config, log *logging.Logger, watchNodeNames []string) (Monitor, error) {
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return Monitor{}, fmt.Errorf("failed to open udp connection for keep alive: %v", err)
	}

	revived := false
	nameToAddr := make(map[string]*net.UDPAddr, len(watchNodeNames))
	addrToName := make(map[string]string, len(watchNodeNames))
	for _, name := range watchNodeNames {
		addr, err := resolveAddr(name, con.HealthCheckPort)
		if err != nil {
			addr, err = revive(name, con.ReviveRetries, log, con.HealthCheckPort)
			revived = true
		}

		if err != nil {
			log.Errorf("Failed to resolve address for node %s after retrying", name)
			continue
		}

		nameToAddr[name] = addr
		addrToName[addr.String()] = name
	}

	if revived {
		time.Sleep(con.ReviveSleepDuration)
	}

	return Monitor{
		con:        con,
		log:        log,
		conn:       conn,
		nameToAddr: nameToAddr,
		addrToName: addrToName,
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

func (m *Monitor) direct(addr *net.UDPAddr) bool {
	_, err := m.conn.WriteToUDP(nil, addr)
	return err == nil
}

func (m *Monitor) broadcast(nodes map[string]string) bool {
	allOk := true
	for _, name := range nodes {
		if !m.direct(m.nameToAddr[name]) {
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
		delete(dead, peerAddr.String())
	}
}

func (m *Monitor) revive(containerName string) (*net.UDPAddr, error) {
	return revive(containerName, m.con.ReviveRetries, m.log, m.con.HealthCheckPort)
}

func (m *Monitor) sendWait(waitDur time.Duration, dead map[string]string) bool {
	m.log.Debugf("Broadcasting keep-alives to %d nodes", len(dead))
	if !m.broadcast(dead) {
		return false
	}

	m.log.Debugf("Waiting for ACKs for %d seconds...", int(waitDur.Seconds()))
	ok, err := m.wait(waitDur, dead)
	if err != nil {
		m.log.Error(err)
	}

	return ok
}

func (m *Monitor) run() {
	dead := make(map[string]string, len(m.addrToName))

	for {
		maps.Copy(dead, m.addrToName)

		waitDur := m.con.StartingKeepAliveWaitDuration
		if !m.sendWait(waitDur, dead) {
			return
		}

		for retry := range m.con.KeepAliveRetries {
			if len(dead) == 0 {
				break
			}

			waitDur <<= 1
			m.log.Infof("[%d] %v didn't respond, retrying in %d seconds", retry+1, slices.Collect(maps.Values(dead)), int(waitDur.Seconds()))

			if !m.sendWait(waitDur, dead) {
				return
			}
		}

		sleepDur := m.con.DefaultSleepDuration
		if len(dead) > 0 {
			m.log.Infof("Dead nodes: %v", slices.Collect(maps.Values(dead)))
			sleepDur = m.con.ReviveSleepDuration
		}

		for oldAddr, name := range dead {
			newAddr, err := m.revive(name)
			if err != nil {
				m.log.Error(err)
				continue
			}

			m.nameToAddr[name] = newAddr
			m.addrToName[newAddr.String()] = name

			delete(m.addrToName, oldAddr)
			delete(dead, oldAddr)
		}

		m.log.Debugf("Sleeping for %d seconds...", int(sleepDur.Seconds()))
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
