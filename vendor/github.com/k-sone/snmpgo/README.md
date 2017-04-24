[![Build Status](https://travis-ci.org/k-sone/snmpgo.svg?branch=master)](https://travis-ci.org/k-sone/snmpgo)
[![GoDoc](https://godoc.org/github.com/k-sone/snmpgo?status.svg)](http://godoc.org/github.com/k-sone/snmpgo)

snmpgo
======

snmpgo is a golang implementation for SNMP.

Supported Message Types
-----------------------

### Sending

* SNMP V1
    - GetRequest
    - GetNextRequest
* SNMP V2c, V3
    - GetRequest
    - GetNextRequest
    - GetBulkRequest
    - V2Trap
    - InformRequest

### Receiving

* SNMP V2c
    - V2Trap
    - InformRequest
* SNMP V3
    - V2Trap

Examples
--------

**[getv2.go](examples/getv2.go), [getv3.go](examples/getv3.go)**

Example for sending a GetRequest.
Explain how to use basic of the API.

```go
package main

import (
    "fmt"

    "github.com/k-sone/snmpgo"
)

func main() {
    snmp, err := snmpgo.NewSNMP(snmpgo.SNMPArguments{
        Version:   snmpgo.V2c,
        Address:   "127.0.0.1:161",
        Retries:   1,
        Community: "public",
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
```

**[trapv2.go](examples/trapv2.go), [trapv3.go](examples/trapv3.go)**

Example for sending a V2Trap.
Explain how to build varbinds using API.

**[multiget.go](examples/multiget.go)**

Example for sending a GetRequest to multiple agents.

**[ifstat.go](examples/ifstat.go)**

This command displays the traffic of agent at regular intervals.
Explain how to process the obtained information.

**[snmpgoget.go](examples/snmpgoget.go)**

[snmpget@Net-SNMP](http://www.net-snmp.org/docs/man/snmpget.html) like command.

**[snmpgobulkwalk.go](examples/snmpgobulkwalk.go)**

[snmpbulkwalk@Net-SNMP](http://www.net-snmp.org/docs/man/snmpbulkwalk.html) like command.

**[snmpgotrap.go](examples/snmpgotrap.go)**

[snmptrap@Net-SNMP](http://www.net-snmp.org/docs/man/snmptrap.html) like command.

Dependencies
------------

- [github.com/geoffgarside/ber](https://github.com/geoffgarside/ber)

License
-------

MIT
