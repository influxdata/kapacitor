## 3.2.0 (2017/03/11)

- Adds ASN.1 BER Unmarshalling [#16](https://github.com/k-sone/snmpgo/pull/16)

## 3.1.0 (2017/03/04)

- Fix data races in closing TrapServer and packetTransport types [#13](https://github.com/k-sone/snmpgo/pull/13)
- Fix OctetString.String() to return human-readable string [#14](https://github.com/k-sone/snmpgo/pull/14)
- Defining a StdLogger interface to allow setting of any logger [#15](https://github.com/k-sone/snmpgo/pull/15)

## 3.0.2 (2016/12/10)

- Fix for data race in generating request ids [#12](https://github.com/k-sone/snmpgo/pull/12)

## 3.0.1 (2016/08/30)

- Fix a bug that denying NoAuth trap, even when server is NoAuth mode

## 3.0.0 (2016/08/27)

- Correction to allow 32 bits compilation [#8](https://github.com/k-sone/snmpgo/pull/8)
- Support for receiving of trap events (V3) [#10](https://github.com/k-sone/snmpgo/pull/10)

#### Breaking Changes
- Change to return an error from `TrapServer.DeleteSecurity`

## 2.0.1 (2016/05/26)

- Raise an error when unmarshalling an invalid SNMP version

## 2.0.0 (2016/02/11)

- Support for receiving of trap events (V2c only) [#4](https://github.com/k-sone/snmpgo/pull/4)

#### Breaking Changes

- Change to return a pointer of xxError struct [#1](https://github.com/k-sone/snmpgo/pull/1)
- Rename `ResponseError` to `MessageError`

## 1.0.1 (2015/07/12)

- Fix validating authoritative engine

## 1.0.0 (2015/01/24)

- Initial release
