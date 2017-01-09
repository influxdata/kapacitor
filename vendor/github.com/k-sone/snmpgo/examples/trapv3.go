package main

import (
	"fmt"

	"github.com/k-sone/snmpgo"
)

func main() {
	// `snmpgo.SNMP.Open` function execute the EngineID Discovery when you use V3.
	// Specify the Agent's EngineID to `snmpgo.SNMPArguments.SecurityEngineId` parameter,
	// if you want to suppress this behavior.
	snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:          snmpgo.V3,
		Address:          "127.0.0.1:162",
		Retries:          1,
		UserName:         "MyName",
		SecurityLevel:    snmpgo.AuthPriv,
		AuthPassword:     "aaaaaaaa",
		AuthProtocol:     snmpgo.Sha,
		PrivPassword:     "bbbbbbbb",
		PrivProtocol:     snmpgo.Aes,
		SecurityEngineId: "8000000004736e6d70676f",
	})
	if err != nil {
		// Failed to create snmpgo.SNMP object
		fmt.Println(err)
		return
	}

	// Build VarBind list
	var varBinds snmpgo.VarBinds
	varBinds = append(varBinds, snmpgo.NewVarBind(snmpgo.OidSysUpTime, snmpgo.NewTimeTicks(1000)))

	oid, _ := snmpgo.NewOid("1.3.6.1.6.3.1.1.5.3")
	varBinds = append(varBinds, snmpgo.NewVarBind(snmpgo.OidSnmpTrap, oid))

	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.2.2.1.1.2")
	varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewInteger(2)))

	oid, _ = snmpgo.NewOid("1.3.6.1.2.1.31.1.1.1.1.2")
	varBinds = append(varBinds, snmpgo.NewVarBind(oid, snmpgo.NewOctetString([]byte("eth0"))))

	if err = snmp.Open(); err != nil {
		// Failed to open connection
		fmt.Println(err)
		return
	}
	defer snmp.Close()

	if err = snmp.V2Trap(varBinds); err != nil {
		// Failed to request
		fmt.Println(err)
		return
	}
}
