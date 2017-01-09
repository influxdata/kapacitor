package main

import (
	"fmt"
	"sync"

	"github.com/k-sone/snmpgo"
)

func get(agent string, oids snmpgo.Oids) {
	// create a SNMP Object for each the agent
	snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
		Version:   snmpgo.V2c,
		Address:   agent,
		Retries:   1,
		Community: "public",
	})
	if err != nil {
		fmt.Printf("[%s] : construct error - %s\n", agent, err)
		return
	}

	if err = snmp.Open(); err != nil {
		fmt.Printf("[%s] : open error - %s\n", agent, err)
		return
	}
	defer snmp.Close()

	pdu, err := snmp.GetRequest(oids)
	if err != nil {
		fmt.Printf("[%s] : get error - %s\n", agent, err)
		return
	}

	if pdu.ErrorStatus() != snmpgo.NoError {
		fmt.Printf("[%s] : error status - %s at %d\n",
			agent, pdu.ErrorStatus(), pdu.ErrorIndex())
	}

	fmt.Printf("[%s] : %s\n", agent, pdu.VarBinds())
}

func main() {
	oids, err := snmpgo.NewOids([]string{
		"1.3.6.1.2.1.1.1.0",
		"1.3.6.1.2.1.1.2.0",
		"1.3.6.1.2.1.1.3.0",
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	agents := []string{
		"192.168.1.1:161",
		"192.168.1.2:161",
		"192.168.1.3:161",
	}

	var wg sync.WaitGroup
	for _, agent := range agents {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			get(a, oids)
		}(agent)
	}
	wg.Wait()
}
