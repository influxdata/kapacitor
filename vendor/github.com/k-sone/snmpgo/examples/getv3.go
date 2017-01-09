package main

import (
	"fmt"

	"github.com/k-sone/snmpgo"
)

func main() {
	snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:       snmpgo.V3,
		Address:       "127.0.0.1:161",
		Retries:       1,
		UserName:      "MyName",
		SecurityLevel: snmpgo.AuthPriv,
		AuthPassword:  "aaaaaaaa",
		AuthProtocol:  snmpgo.Sha,
		PrivPassword:  "bbbbbbbb",
		PrivProtocol:  snmpgo.Aes,
	})
	if err != nil {
		// Failed to create snmpgo.SNMP object
		fmt.Println(err)
		return
	}

	oids, err := snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.3.0",
	})
	if err != nil {
		// Failed to parse Oids
		fmt.Println(err)
		return
	}

	if err = snmp.Open(); err != nil {
		// Failed to open connection
		fmt.Println(err)
		return
	}
	defer snmp.Close()

	pdu, err := snmp.GetRequest(oids)
	if err != nil {
		// Failed to request
		fmt.Println(err)
		return
	}
	if pdu.ErrorStatus() != snmpgo.NoError {
		// Received an error from the agent
		fmt.Println(pdu.ErrorStatus(), pdu.ErrorIndex())
	}

	// get VarBind list
	fmt.Println(pdu.VarBinds())

	// select a VarBind
	fmt.Println(pdu.VarBinds().MatchOid(oids[0]))
}
