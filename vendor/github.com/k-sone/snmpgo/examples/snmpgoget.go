package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/k-sone/snmpgo"
)

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
		fmt.Fprintf(os.Stderr, "Usage of %s: [OPTIONS] AGENT OID [OID]..\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "AGENT:\n  hostname:port or ip-address:port\n")
		fmt.Fprintf(os.Stderr, "OPTIONS:\n")
		flag.PrintDefaults()
	}

	protocol := flag.String("p", "udp", "Protocol (udp|udp6|tcp|tcp6)")
	timeout := flag.Uint("t", 5, "Request timeout (number of seconds)")
	retries := flag.Uint("r", 1, "Number of retries")
	version := flag.String("v", "1", "SNMP version to use (1|2c|3)")
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
	case "1":
		args.Version = snmpgo.V1
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

	return args, flag.Args()
}

func main() {
	snmpArgs, cmdArgs := parseArgs()
	if len(cmdArgs) < 2 {
		usage("required AGENT and OID", 2)
	}

	oids, err := snmpgo.NewOids(cmdArgs[1:])
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

	pdu, err := snmp.GetRequest(oids)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if pdu.ErrorStatus() != snmpgo.NoError {
		fmt.Fprintln(os.Stderr, pdu.ErrorStatus(), pdu.ErrorIndex())
		os.Exit(1)
	}

	for _, val := range pdu.VarBinds() {
		fmt.Printf("%s = %s: %s\n", val.Oid, val.Variable.Type(), val.Variable)
	}
}
