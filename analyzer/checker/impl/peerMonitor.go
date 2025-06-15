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

const DEFAULT_SLEEP_DURATION = 5
const RESPAWN_SLEEP_DURATION = 5
const CHECKER_COMPOSE_FILENAME = "compose.checkers.yaml"
const RESPAWN_RETRIES = 5
const KEEP_ALIVE_RETRIES = 5
const FIRST_KEEP_ALIVE_WAIT = 5
const HOST_FMT = "health-checker-%d"

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
	host := fmt.Sprintf(HOST_FMT, nextId)
	addrStr := fmt.Sprintf("%s:%d", host, con.KeepAlivePort)
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
		defaultSleepDur := time.Duration(DEFAULT_SLEEP_DURATION) * time.Second
		respawnSleepDur := time.Duration(RESPAWN_SLEEP_DURATION) * time.Second

		for {
			if err := m.sendNext(); err != nil {
				break
			}

			retries := 0
			waitDur := time.Duration(FIRST_KEEP_ALIVE_WAIT) * time.Second
			for retries <= KEEP_ALIVE_RETRIES {
				if m.waitNext(waitDur) {
					break
				}

				retries++
				waitDur <<= 1
			}

			if m.exit {
				break
			}

			sleepDur := defaultSleepDur
			if retries > KEEP_ALIVE_RETRIES {
				m.respawnNext(string(m.nextAddr.IP))
				sleepDur = respawnSleepDur
			}

			time.Sleep(sleepDur)
		}
	}()

	return nil
}

func (m *PeerMonitor) respawnNext(containerName string) error {
	cmd := exec.Command("docker", "kill", "--signal=9", containerName)
	cmd.Run()

	cmd = exec.Command("docker", "compose", "-f", CHECKER_COMPOSE_FILENAME, "up", "-d", containerName)
	err := cmd.Run()

	for i := 0; err != nil && i < RESPAWN_RETRIES; i++ {
		err = cmd.Run()
	}

	if err != nil {
		return fmt.Errorf("failed to respawn %s: %v", containerName, err)
	}

	return nil
}

func (m *PeerMonitor) Close() error {
	m.exit = true
	err := m.conn.Close()
	m.wg.Wait()
	m.log.Infof("Terminating Peer Monitor...")
	return err
}
