package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/big"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/k-sone/snmpgo"
)

var sysDescr *snmpgo.Oid = snmpgo.MustNewOid("1.3.6.1.2.1.1.1")
var ifAdminStatus *snmpgo.Oid = snmpgo.MustNewOid("1.3.6.1.2.1.2.2.1.7")
var ifName *snmpgo.Oid = snmpgo.MustNewOid("1.3.6.1.2.1.31.1.1.1.1")
var ifHCInOctets *snmpgo.Oid = snmpgo.MustNewOid("1.3.6.1.2.1.31.1.1.1.6")
var ifHCOutOctets *snmpgo.Oid = snmpgo.MustNewOid("1.3.6.1.2.1.31.1.1.1.10")

var delay time.Duration = 30 * time.Second
var iterations int = -1
var maxRepetitions int
var errMessage string
var ifRegexp *regexp.Regexp
var noHeader bool

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
		fmt.Fprintf(os.Stderr, "Usage of %s: [OPTIONS] AGENT [DELAY [COUNT]]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "AGENT:\n  hostname:port or ip-address:port\n")
		fmt.Fprintf(os.Stderr, "DELAY:\n  the delay between updates in seconds (default: 10)\n")
		fmt.Fprintf(os.Stderr, "COUNT:\n  the number of updates (default: infinity)\n")
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
	ifRegString := flag.String("ifreg", "", "Regular expression that specifies the interface")
	flag.IntVar(&maxRepetitions, "Cr", 10, "Max repetitions")
	flag.BoolVar(&noHeader, "noheader", false, "Disable header line")

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

	if *ifRegString != "" {
		var err error
		if ifRegexp, err = regexp.Compile(*ifRegString); err != nil {
			usage(fmt.Sprintf("Illegal Regular expression - %s", err), 2)
		}
	}

	return args, flag.Args()
}

type ifInfo struct {
	descr     string
	IfList    []int
	IfName    map[int]string
	IfIn      map[int]*big.Int
	IfOut     map[int]*big.Int
	LastIfIn  map[int]*big.Int
	LastIfOut map[int]*big.Int
	Time      time.Time
	LastTime  time.Time
}

func newIfInfo() *ifInfo {
	return &ifInfo{
		IfList:    make([]int, 0),
		IfName:    make(map[int]string),
		IfIn:      make(map[int]*big.Int),
		IfOut:     make(map[int]*big.Int),
		LastIfIn:  make(map[int]*big.Int),
		LastIfOut: make(map[int]*big.Int),
	}
}

func getIfInfo(snmp *snmpgo.SNMP) (*ifInfo, error) {
	pdu, err := snmp.GetBulkWalk(snmpgo.Oids{sysDescr, ifAdminStatus, ifName}, 1, maxRepetitions)
	if err != nil {
		return nil, err
	}

	if pdu.ErrorStatus() != snmpgo.NoError {
		return nil, fmt.Errorf(
			"failed to get system information - %s(%d)", pdu.ErrorStatus(), pdu.ErrorIndex())
	}

	info := newIfInfo()
	if descrs := pdu.VarBinds().MatchBaseOids(sysDescr); len(descrs) > 0 {
		info.descr = descrs[0].Variable.String()
	}

	ifNameDepth := len(ifName.Value)
	for _, varBind := range pdu.VarBinds().MatchBaseOids(ifName) {
		// get interface number and name
		if len(varBind.Oid.Value) != ifNameDepth+1 {
			continue
		}
		ifNum := varBind.Oid.Value[ifNameDepth]
		ifName := varBind.Variable.String()

		if ifRegexp != nil && !ifRegexp.MatchString(ifName) {
			continue
		}

		// get admin status
		oid, err := ifAdminStatus.AppendSubIds([]int{ifNum})
		if err != nil {
			return nil, err
		}

		// check admin status("1" is up)
		if adms := pdu.VarBinds().MatchOid(oid); adms == nil || adms.Variable.String() != "1" {
			continue
		}

		info.IfList = append(info.IfList, ifNum)
		info.IfName[ifNum] = ifName
		info.IfIn[ifNum] = nil
		info.IfOut[ifNum] = nil
		info.LastIfIn[ifNum] = nil
		info.LastIfOut[ifNum] = nil
	}

	if len(info.IfList) == 0 {
		return nil, fmt.Errorf("no interface")
	}

	return info, nil
}

func getIfTraffic(snmp *snmpgo.SNMP, info *ifInfo) error {
	pdu, err := snmp.GetBulkWalk(snmpgo.Oids{ifHCInOctets, ifHCOutOctets}, 0, maxRepetitions)
	if err != nil {
		return err
	}

	if pdu.ErrorStatus() != snmpgo.NoError {
		return fmt.Errorf(
			"failed to get network traffic - %s(%d)", pdu.ErrorStatus(), pdu.ErrorIndex())
	}

	for _, ifNum := range info.IfList {
		oid, err := ifHCInOctets.AppendSubIds([]int{ifNum})
		if err != nil {
			return err
		}
		if in := pdu.VarBinds().MatchOid(oid); in != nil {
			count, err := in.Variable.BigInt()
			if err != nil {
				return err
			}
			info.LastIfIn[ifNum], info.IfIn[ifNum] = info.IfIn[ifNum], count
		} else {
			info.LastIfIn[ifNum], info.IfIn[ifNum] = info.IfIn[ifNum], nil
		}

		oid, err = ifHCOutOctets.AppendSubIds([]int{ifNum})
		if err != nil {
			return err
		}
		if out := pdu.VarBinds().MatchOid(oid); out != nil {
			count, err := out.Variable.BigInt()
			if err != nil {
				return err
			}
			info.LastIfOut[ifNum], info.IfOut[ifNum] = info.IfOut[ifNum], count
		} else {
			info.LastIfOut[ifNum], info.IfOut[ifNum] = info.IfOut[ifNum], nil
		}
	}

	info.LastTime, info.Time = info.Time, time.Now()
	return nil
}

func printHeader(info *ifInfo) {
	var buf bytes.Buffer

	if info.descr != "" {
		buf.WriteString(info.descr)
		buf.WriteString("\n")
	}
	for _, ifNum := range info.IfList {
		buf.WriteString(fmt.Sprintf("%-23s ", info.IfName[ifNum]))
	}
	buf.WriteString("\n")
	for _ = range info.IfList {
		buf.WriteString("in          out         ")
	}
	fmt.Println(buf.String())
}

func printTraffic(info *ifInfo) {
	// TODO wrap around support
	var buf bytes.Buffer
	for _, ifNum := range info.IfList {
		var in, out big.Int
		if info.IfIn[ifNum] != nil && info.LastIfIn[ifNum] != nil {
			in.Sub(info.IfIn[ifNum], info.LastIfIn[ifNum])
			buf.WriteString(fmt.Sprintf("%-11d ", in.Uint64()))
		} else {
			buf.WriteString("-           ")
		}
		if info.IfOut[ifNum] != nil && info.LastIfOut[ifNum] != nil {
			out.Sub(info.IfOut[ifNum], info.LastIfOut[ifNum])
			buf.WriteString(fmt.Sprintf("%-11d ", out.Uint64()))
		} else {
			buf.WriteString("-           ")
		}
	}
	fmt.Println(buf.String())
}

func main() {
	snmpArgs, cmdArgs := parseArgs()
	if l := len(cmdArgs); l < 1 {
		usage("required AGENT", 2)
	} else {
		if l > 1 {
			if num, err := strconv.Atoi(cmdArgs[1]); err != nil || num < 1 {
				usage(fmt.Sprintf("Illegal delay, value `%s`", cmdArgs[1]), 2)
			} else {
				delay = time.Duration(num) * time.Second
			}
		}
		if l > 2 {
			if num, err := strconv.Atoi(cmdArgs[2]); err != nil || num < 1 {
				usage(fmt.Sprintf("Illegal iterations, value `%s`", cmdArgs[2]), 2)
			} else {
				iterations = num
			}
		}
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

	info, err := getIfInfo(snmp)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if !noHeader {
		printHeader(info)
	}

	start := time.Now()
	for i := 0; ; i++ {
		if err := getIfTraffic(snmp, info); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		printTraffic(info)

		if iterations > 0 && i >= iterations {
			break
		}

		start = start.Add(delay)
		if d := start.Sub(info.Time); d > 0 {
			time.Sleep(d)
		}
	}
}
