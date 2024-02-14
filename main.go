package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	h "github.com/a4lex/helper"
	"github.com/gosnmp/gosnmp"
	g "github.com/gosnmp/gosnmp"
	"net.donbass.ipif2db/pkg/client/mysql"
)

func init() {
LOOP:
	for i, arg := range os.Args[1:] {
		switch {
		case arg == "-c" || arg == "--config":
			if i+2 < len(os.Args) {
				configPath := os.Args[i+2]
				configName := strings.Split(os.Args[0], "/")[len(strings.Split(os.Args[0], "/"))-1]
				//TODO
				configName = "ipif2db"
				if err := h.ConfigInit(configPath, configName); err != nil {
					panic(err)
				}
				break LOOP
			}
		case arg == "-h" || arg == "--help":
			fmt.Printf("Usage of %q:\n  -c/--config\t- config file\n  -h/--help\t- print this\n", os.Args[0])
			os.Exit(0)
		}
	}
}

const MYSQL_GET_DEVICE_LIST = "SELECT id, name, INET_NTOA(ip) AS ip, snmp_community, snmp_version, NOW() AS curr_time FROM router"
const MYSQL_CREATE_IFACE_NETWORK = "" +
	"INSERT INTO iface_network (routerid, ifname, network, mask, closed_at) " +
	"VALUES (?, ?, INET_ATON(?) & INET_ATON(?), INET_ATON(?), '0000-00-00 00:00:00') " +
	"ON DUPLICATE KEY UPDATE updated_at = ?"
const MYSQL_CLOSE_IFACE_NETWORK = "" +
	"UPDATE iface_network SET closed_at = ? WHERE updated_at != ? AND routerid = ? AND closed_at='0000-00-00 00:00:00' " +
	"AND (SELECT SUM(IF(updated_at = ?, 1,0)) > SUM(IF(updated_at != ?, 1,0)) FROM iface_network WHERE routerid=? LIMIT 1)"

const OID_IPAD2IFINDEX = ".1.3.6.1.2.1.4.20.1.2"
const OID_IPAD2NETMASK = ".1.3.6.1.2.1.4.20.1.3"
const OID_IFNAME = ".1.3.6.1.2.1.31.1.1.1.1"

type IfaceNetwork struct {
	IfName string
	Ip     string
	Mask   string
}

func main() {
	// init log
	h.LogInit(h.CFG.String("log.file", fmt.Sprintf("%s.log", os.Args[0])), h.CFG.Int("log.level", 4095))
	defer h.LogRelease()

	// connect to mysql - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
	sqlHost, sqlUsername, sqlPassword, sqlDatabase, sqlDSN, sqlIdle, sqlOpen, sqlLife, logSQL :=
		h.CFG.String("mysqldb.host", "tcp(127.0.0.1:3306)"),
		h.CFG.String("mysqldb.user", "root"),
		h.CFG.String("mysqldb.password", ""),
		h.CFG.String("mysqldb.database", ""),
		h.CFG.String("mysqldb.dsn", "?allowOldPasswords=true"),
		h.CFG.Int("mysqldb.maxidle", 32),
		h.CFG.Int("mysqldb.maxopen", 32),
		h.CFG.Int("mysqldb.maxlife", 60),
		h.CustomLogFunc(64, "MSQL")

	ctxSQL, cancelSQL := context.WithTimeout(context.Background(), time.Duration(time.Second*3))
	defer cancelSQL()

	mysqlDB, err := mysql.NewClient(ctxSQL, sqlHost, sqlUsername, sqlPassword,
		sqlDatabase, sqlDSN, sqlIdle, sqlOpen, sqlLife, &logSQL)
	if err != nil {
		h.Fatal("can't connect to mysql: %v", err.Error())
	}
	defer mysqlDB.Disconnect()
	// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

	deviceList := make([]struct {
		ID        string `sql:"id" `
		Name      string `sql:"name"`
		Ip        string `sql:"ip"`
		Community string `sql:"snmp_community"`
		Version   string `sql:"snmp_version"`
		CurrTime  string `sql:"curr_time"`
	}, 0)

	if err := mysqlDB.SelectMany(context.TODO(), &deviceList, MYSQL_GET_DEVICE_LIST); err != nil {
		h.Fatal("can not select system names: %v", err)
	}

	for _, dev := range deviceList {

		snmpQuery := make([]string, 0)
		iface2networkMap := make(map[string]IfaceNetwork)

		g.Default.Target = dev.Ip
		g.Default.Community = dev.Community
		if err := g.Default.Connect(); err != nil {
			h.Error("connect() err: %v", err)
			continue
		} else {
			h.Info("connected to %s [%s]", dev.Name, dev.Ip)
		}
		defer g.Default.Conn.Close()

		snmpQuery = []string{OID_IPAD2IFINDEX, OID_IPAD2NETMASK}

		for {
			if snmpResult, err := g.Default.GetNext(snmpQuery); err != nil || len(snmpResult.Variables) != 2 || !strings.HasPrefix(snmpResult.Variables[0].Name, OID_IPAD2IFINDEX) {
				h.Info("finished to %s [%s]", dev.Name, dev.Ip)
				break
			} else {
				for id, snmpPDU := range snmpResult.Variables {
					snmpQuery[id] = snmpPDU.Name
				}

				iface2networkMap[gosnmp.ToBigInt(snmpResult.Variables[0].Value).String()] = IfaceNetwork{
					Ip:   strings.Replace(snmpResult.Variables[0].Name, OID_IPAD2IFINDEX+".", "", 1),
					Mask: fmt.Sprintf("%s", snmpResult.Variables[1].Value),
				}
			}
		}

		for ifId, net := range iface2networkMap {
			if res, err := g.Default.Get([]string{fmt.Sprintf("%s.%s", OID_IFNAME, ifId)}); err != nil || len(res.Variables) != 1 {
				h.Error("error on snmp-get reuest: %s", err)
			} else {
				ifName := string(res.Variables[0].Value.([]byte))
				mysqlDB.Query(context.TODO(), MYSQL_CREATE_IFACE_NETWORK, dev.ID, ifName, net.Ip, net.Mask, net.Mask, dev.CurrTime)
			}
		}
		mysqlDB.Query(context.TODO(), MYSQL_CLOSE_IFACE_NETWORK, dev.CurrTime, dev.CurrTime, dev.ID, dev.CurrTime, dev.CurrTime, dev.ID)
	}

}
