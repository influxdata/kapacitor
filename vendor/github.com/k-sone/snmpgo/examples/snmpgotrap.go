package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/k-sone/snmpgo"
)

var hexPrefix *regexp.Regexp = regexp.MustCompile(`^0[xX]`)
var inform bool
var engineBoots int
var engineTime int
var errMessage string

func usage(msg string, code int) {
	errMessage = msg
	flag.Usage()
	os.Exit(code)
}

func parseArgs() (*snmpgo.SNMPArguments, []string) {
	flag.Usage = func() {
		if errMessage != "" {
			fmt.Fprintf(os.Stderr, "%s\n", errMessage)
		}
		fmt.Fprintf(os.Stderr,
			"Usage of %s: [OPTIONS] AGENT UPTIME TRAP-OID [OID TYPE VALUE]..\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "AGENT:\n  hostname:port or ip-address:port\n")
		fmt.Fprintf(os.Stderr, "UPTIME:\n  system uptime\n")
		fmt.Fprintf(os.Stderr, "TYPE:\n")
		fmt.Fprintf(os.Stderr, "  i - INTEGER   u - UNSIGNED   c - COUNTER32 C - COUNTER64\n")
		fmt.Fprintf(os.Stderr, "  t - TIMETICKS a - IPADDRESS  o - OID       n - NULL\n")
		fmt.Fprintf(os.Stderr, "  s - STRING    x - HEX STRING d - DECIMAL STRING\n")
		fmt.Fprintf(os.Stderr, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	protocol := flag.String("p", "udp", "Protocol (udp|udp6|tcp|tcp6)")
	timeout := flag.Uint("t", 5, "Request timeout (number of seconds)")
	retries := flag.Uint("r", 1, "Number of retries")
	version := flag.String("v", "2c", "SNMP version to use (2c|3)")
	community := flag.String("c", "", "Community")
	username := flag.String("u", "", "Security name")
	seclevel := flag.String("l", "NoAuthNoPriv", "Security level (NoAuthNoPriv|AuthNoPriv|AuthPriv)")
	authproto := flag.String("a", "", "Authentication protocol (MD5|SHA)")
	authpass := flag.String("A", "", "Authentication protocol pass phrase")
	privproto := flag.String("x", "", "Privacy protocol (DES|AES)")
	privpass := flag.String("X", "", "Privacy protocol pass phrase")
	secengine := flag.String("e", "", "Security engine ID")
	contextengine := flag.String("E", "", "Context engine ID")
	contextname := flag.String("n", "", "Context name")
	bootsTime := flag.String("Z", "", "EngineBoots and EngineTime (boots,time)")
	flag.BoolVar(&inform, "Ci", false, "Send an Inform")

	flag.Parse()

	args := &snmpgo.SNMPArguments{
		Network:          *protocol,
		Address:          flag.Arg(0),
		Timeout:          time.Duration(*timeout) * time.Second,
		Retries:          *retries,
		Community:        *community,
		UserName:         *username,
		AuthPassword:     *authpass,
		AuthProtocol:     snmpgo.AuthProtocol(*authproto),
		PrivPassword:     *privpass,
		PrivProtocol:     snmpgo.PrivProtocol(*privproto),
		SecurityEngineId: *secengine,
		ContextEngineId:  *contextengine,
		ContextName:      *contextname,
	}

	switch *version {
	case "2c":
		args.Version = snmpgo.V2c
	case "3":
		args.Version = snmpgo.V3
	default:
		usage(fmt.Sprintf("Illegal Version, value `%s`", *version), 2)
	}

	switch *seclevel {
	case "NoAuthNoPriv":
		args.SecurityLevel = snmpgo.NoAuthNoPriv
	case "AuthNoPriv":
		args.SecurityLevel = snmpgo.AuthNoPriv
	case "AuthPriv":
		args.SecurityLevel = snmpgo.AuthPriv
	default:
		usage(fmt.Sprintf("Illegal SecurityLevel, value `%s`", *seclevel), 2)
	}

	if *bootsTime != "" {
		var success bool
		if engineBoots, engineTime, success = parseBootsTime(*bootsTime); !success {
			usage(fmt.Sprintf("Invalid Boots,Time format, value `%s`", *bootsTime), 2)
		}
	}

	return args, flag.Args()
}

func parseBootsTime(bt string) (int, int, bool) {
	s := strings.Split(bt, ",")
	if len(s) == 2 {
		b, e1 := strconv.Atoi(s[0])
		t, e2 := strconv.Atoi(s[1])
		if e1 == nil && e2 == nil {
			return b, t, true
		}
	}
	return 0, 0, false
}

func getUptime(s string) uint32 {
	if uptime, err := strconv.ParseUint(s, 10, 32); err == nil {
		return uint32(uptime)
	}

	// The syscall.Sysinfo only works on Linux
	//	var info syscall.Sysinfo_t
	//	if err := syscall.Sysinfo(&info); err == nil {
	//		return uint32(info.Uptime * 100)
	//	}

	return 0
}

func buildVariable(kind string, value string) (val snmpgo.Variable, err error) {
	switch kind {
	case "i":
		var num int64
		if num, err = strconv.ParseInt(value, 10, 32); err == nil {
			val = snmpgo.NewInteger(int32(num))
		}
	case "u":
		var num uint64
		if num, err = strconv.ParseUint(value, 10, 32); err == nil {
			val = snmpgo.NewGauge32(uint32(num))
		}
	case "c":
		var num uint64
		if num, err = strconv.ParseUint(value, 10, 32); err == nil {
			val = snmpgo.NewCounter32(uint32(num))
		}
	case "C":
		var num uint64
		if num, err = strconv.ParseUint(value, 10, 64); err == nil {
			val = snmpgo.NewCounter64(num)
		}
	case "t":
		var num uint64
		if num, err = strconv.ParseUint(value, 10, 32); err == nil {
			val = snmpgo.NewTimeTicks(uint32(num))
		}
	case "a":
		if ip := net.ParseIP(value); ip != nil && len(ip) == 4 {
			val = snmpgo.NewIpaddress(ip[0], ip[1], ip[2], ip[3])
		} else {
			return nil, fmt.Errorf("%s: no valid IP Address", value)
		}
	case "o":
		val, err = snmpgo.NewOid(value)
	case "n":
		val = snmpgo.NewNull()
	case "s":
		val = snmpgo.NewOctetString([]byte(value))
	case "x":
		var b []byte
		hx := hexPrefix.ReplaceAllString(value, "")
		if b, err = hex.DecodeString(hx); err == nil {
			val = snmpgo.NewOctetString(b)
		} else {
			return nil, fmt.Errorf("%s: no valid Hex String", value)
		}
	case "d":
		s := strings.Split(value, ".")
		b := make([]byte, len(s))
		for i, piece := range s {
			var num int
			if num, err = strconv.Atoi(piece); err != nil || num > 0xff {
				return nil, fmt.Errorf("%s: no valid Decimal String", value)
			}
			b[i] = byte(num)
		}
		val = snmpgo.NewOctetString(b)
	default:
		return nil, fmt.Errorf("%s: unknown TYPE", kind)
	}

	return
}

func buildVarBinds(cmdArgs []string) (snmpgo.VarBinds, error) {
	var varBinds snmpgo.VarBinds

	uptime := snmpgo.NewTimeTicks(getUptime(cmdArgs[1]))
	varBinds = append(varBinds, snmpgo.NewVarBind(snmpgo.OidSysUpTime, uptime))

	oid, err := snmpgo.NewOid(cmdArgs[2])
	if err != nil {
		return nil, err
	}
	varBinds = append(varBinds, snmpgo.NewVarBind(snmpgo.OidSnmpTrap, oid))

	for i := 3; i < len(cmdArgs); i += 3 {
		oid, err := snmpgo.NewOid(cmdArgs[i])
		if err != nil {
			return nil, err
		}

		val, err := buildVariable(cmdArgs[i+1], cmdArgs[i+2])
		if err != nil {
			return nil, err
		}

		varBinds = append(varBinds, snmpgo.NewVarBind(oid, val))
	}

	return varBinds, nil
}

func main() {
	snmpArgs, cmdArgs := parseArgs()
	if l := len(cmdArgs); l < 3 {
		usage("required AGENT and UPTIME and TRAP-OID", 2)
	} else if l%3 != 0 {
		usage(fmt.Sprintf("%s: missing TYPE/VALUE for variable", cmdArgs[l/3*3]), 2)
	}

	varBinds, err := buildVarBinds(cmdArgs)
	if err != nil {
		usage(err.Error(), 2)
	}

	snmp, err := snmpgo.NewSNMP(*snmpArgs)
	if err != nil {
		usage(err.Error(), 2)
	}
	if err = snmp.Open(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer snmp.Close()

	if inform {
		err = snmp.InformRequest(varBinds)
	} else {
		err = snmp.V2TrapWithBootsTime(varBinds, engineBoots, engineTime)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
