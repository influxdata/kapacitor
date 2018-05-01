package influxdb_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	influxcli "github.com/influxdata/kapacitor/influxdb"
	"github.com/influxdata/kapacitor/services/diagnostic"
	"github.com/influxdata/kapacitor/services/httpd"
	"github.com/influxdata/kapacitor/services/influxdb"
	"github.com/influxdata/kapacitor/uuid"
)

const (
	randomTokenData = "test random data that is 64 bytes long xxxxxxxxxxxxxxxxxxxxxxxxx"
	testClusterName = "testcluster0"
	randomToken     = testClusterName + ";" + randomTokenData

	testDestination = "http://localhost:9092"

	subMode   = "ANY"
	tokenSize = 64
)

var (
	testKapacitorClusterID = uuid.New()
	testSubName            = "kapacitor-" + testKapacitorClusterID.String()
)

var diagService *diagnostic.Service

func init() {
	diagService = diagnostic.NewService(diagnostic.NewConfig(), ioutil.Discard, ioutil.Discard)
	diagService.Open()
}

func init() {
	if len(randomTokenData) != tokenSize {
		panic(fmt.Sprintf("invalid randomTokenData: got %d exp %d", len(randomTokenData), tokenSize))
	}
}

func tokenForCluster(clusterName string) string {
	return clusterName + ";" + randomTokenData
}

func testDestinationWithTokenForCluster(clusterName string) string {
	return "http://~subscriber:" + base64.RawURLEncoding.EncodeToString([]byte(tokenForCluster(clusterName))) + "@localhost:9092"
}
func testDestinationsWithTokensForCluster(clusterName string) []interface{} {
	return []interface{}{testDestinationWithTokenForCluster(clusterName)}
}

type tokenGrant struct {
	token string
	db    string
	rp    string
}

func TestService_Open_LinkSubscriptions(t *testing.T) {
	type clusterInfo struct {
		name  string
		dbrps map[string][]string
		// Map of db names to list of rp that have subs
		subs map[string][]string
	}
	type subChanged struct {
		// All the ways a sub can change
		NoDests       bool
		InvalidURL    bool
		WrongProtocol bool
		Host          bool
		Port          bool
		ExtraUser     bool
		NoUser        bool
		WrongUser     bool
		NoPassword    bool
		WrongCluster  bool
	}
	type partialConfig struct {
		configSubs   map[string][]string
		configExSubs map[string][]string
	}
	testCases := map[string]struct {
		useTokens bool

		// First round
		clusters      map[string]clusterInfo
		tokens        []string
		createSubs    []string
		dropSubs      []string
		grantedTokens []tokenGrant
		revokedTokens []string
		subChanged    subChanged

		// apply new config between rounds
		partialConfigs map[string]partialConfig

		// Second round
		secondClusters      map[string]clusterInfo
		secondTokens        []string
		secondCreateSubs    []string
		secondDropSubs      []string
		secondGrantedTokens []tokenGrant
		secondRevokedTokens []string
		secondSubChanged    subChanged
	}{
		"NoExisting": {
			useTokens: true,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				}},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpB DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db2.rpC DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			grantedTokens: []tokenGrant{
				{
					token: randomToken,
					db:    "db1",
					rp:    "rpA",
				},
				{
					token: randomToken,
					db:    "db1",
					rp:    "rpB",
				},
				{
					token: randomToken,
					db:    "db2",
					rp:    "rpC",
				},
			},
		},
		"NoExisting_NoTokens": {
			useTokens: false,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				}},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestination + `'`,
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpB DESTINATIONS ANY '` + testDestination + `'`,
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db2.rpC DESTINATIONS ANY '` + testDestination + `'`,
			},
		},
		"Existing": {
			useTokens: true,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				}},
			tokens: []string{
				randomToken,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpB DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db2.rpC DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			grantedTokens: []tokenGrant{
				{
					token: randomToken,
					db:    "db1",
					rp:    "rpB",
				},
				{
					token: randomToken,
					db:    "db2",
					rp:    "rpC",
				},
			},
		},
		"NoChanges": {
			useTokens: true,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				}},
			tokens: []string{
				randomToken,
			},
		},
		"ExtraTokens": {
			useTokens: true,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				},
				"anothercluster": {
					dbrps: map[string][]string{
						"db10": []string{"rpZ"},
					},
					subs: map[string][]string{
						"db10": []string{"rpZ"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				},
				"anothercluster": {
					dbrps: map[string][]string{
						"db10": []string{"rpZ"},
					},
					subs: map[string][]string{
						"db10": []string{"rpZ"},
					},
				},
			},
			tokens: []string{
				randomToken,
				"invalidtoken",
				tokenForCluster("anothercluster"),
				testClusterName + ";unusedtoken",
			},
			revokedTokens: []string{
				"invalidtoken",
				testClusterName + ";unusedtoken",
			},
		},
		"ExtraNonClusterTokens": {
			useTokens: true,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				},
				"anothercluster": {
					dbrps: map[string][]string{
						"db10": []string{"rpZ"},
					},
					subs: map[string][]string{
						"db10": []string{"rpZ"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC"},
					},
				},
				"anothercluster": {
					dbrps: map[string][]string{
						"db10": []string{"rpZ"},
					},
					subs: map[string][]string{
						"db10": []string{"rpZ"},
					},
				},
			},
			tokens: []string{
				randomToken,
				"invalidtoken",
				tokenForCluster("anothercluster"),
				testClusterName + ";unusedtoken",
				tokenForCluster("nonexistantcluster"),
			},
			revokedTokens: []string{
				"invalidtoken",
				testClusterName + ";unusedtoken",
				tokenForCluster("nonexistantcluster"),
			},
		},
		"SecondDroppedDB": {
			useTokens: true,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					// db1 had been dropped
				}},
			tokens: []string{
				randomToken,
			},
			secondTokens: []string{
				randomToken,
			},
			secondRevokedTokens: []string{
				randomToken,
			},
		},
		"SecondNewDB": {
			useTokens: true,
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
						"db2": []string{"rpB"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			tokens: []string{
				randomToken,
			},
			secondTokens: []string{
				randomToken,
			},
			secondCreateSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db2.rpB DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			secondGrantedTokens: []tokenGrant{{
				token: randomToken,
				db:    "db2",
				rp:    "rpB",
			}},
		},
		"SC_NoDests": {
			useTokens:  false,
			subChanged: subChanged{NoDests: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestination + `'`,
			},
		},
		"SC_InvalidURL": {
			useTokens:  true,
			subChanged: subChanged{InvalidURL: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			tokens: []string{
				randomToken,
			},
			secondTokens: []string{
				randomToken,
			},
			grantedTokens: []tokenGrant{{
				token: randomToken,
				db:    "db1",
				rp:    "rpA",
			}},
		},
		"SC_WrongProtocol": {
			useTokens:  false,
			subChanged: subChanged{WrongProtocol: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestination + `'`,
			},
		},
		"SC_Host": {
			useTokens:  false,
			subChanged: subChanged{Host: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestination + `'`,
			},
		},
		"SC_Port": {
			useTokens:  false,
			subChanged: subChanged{Port: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestination + `'`,
			},
		},
		"SC_ExtraUser": {
			useTokens:  false,
			subChanged: subChanged{ExtraUser: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestination + `'`,
			},
		},
		"SC_NoUser": {
			useTokens:  true,
			subChanged: subChanged{NoUser: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			tokens: []string{
				randomToken,
			},
			secondTokens: []string{
				randomToken,
			},
			grantedTokens: []tokenGrant{{
				token: randomToken,
				db:    "db1",
				rp:    "rpA",
			}},
		},
		"SC_WrongUser": {
			useTokens:  true,
			subChanged: subChanged{WrongUser: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			tokens: []string{
				randomToken,
			},
			secondTokens: []string{
				randomToken,
			},
			grantedTokens: []tokenGrant{{
				token: randomToken,
				db:    "db1",
				rp:    "rpA",
			}},
		},
		"SC_NoPassword": {
			useTokens:  true,
			subChanged: subChanged{NoPassword: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			tokens: []string{
				randomToken,
			},
			secondTokens: []string{
				randomToken,
			},
			grantedTokens: []tokenGrant{{
				token: randomToken,
				db:    "db1",
				rp:    "rpA",
			}},
		},
		"SC_WrongCluster": {
			useTokens:  true,
			subChanged: subChanged{WrongCluster: true},
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA"},
					},
				}},
			dropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
			createSubs: []string{
				`CREATE SUBSCRIPTION "` + testSubName + `" ON db1.rpA DESTINATIONS ANY '` + testDestinationWithTokenForCluster(testClusterName) + `'`,
			},
			tokens: []string{
				randomToken,
			},
			secondTokens: []string{
				randomToken,
			},
			grantedTokens: []tokenGrant{{
				token: randomToken,
				db:    "db1",
				rp:    "rpA",
			}},
		},
		"ConfigChange_NewSubs": {
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
				},
			},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
				},
			},
			partialConfigs: map[string]partialConfig{
				testClusterName: {
					configSubs: map[string][]string{
						"db1": {"rpA"},
					},
				},
			},
			secondDropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpB`,
				`DROP SUBSCRIPTION "` + testSubName + `" ON db2.rpC`,
				`DROP SUBSCRIPTION "` + testSubName + `" ON db2.rpD`,
			},
		},
		"ConfigChange_NewExcludes": {
			clusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
				}},
			secondClusters: map[string]clusterInfo{
				testClusterName: {
					dbrps: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
					subs: map[string][]string{
						"db1": []string{"rpA", "rpB"},
						"db2": []string{"rpC", "rpD"},
					},
				}},
			partialConfigs: map[string]partialConfig{
				testClusterName: {
					configExSubs: map[string][]string{
						"db1": {"rpA"},
					},
				},
			},
			secondDropSubs: []string{
				`DROP SUBSCRIPTION "` + testSubName + `" ON db1.rpA`,
			},
		},
	}
	for testName, tc := range testCases {
		t.Log("starting test:", testName)
		log.Println("starting test:", testName)
		clusterNames := make([]string, 0, len(tc.clusters))
		clusterNameLookup := make(map[string]int, len(tc.clusters))
		i := 0
		for clusterName := range tc.clusters {
			clusterNames = append(clusterNames, clusterName)
			clusterNameLookup[clusterName] = i
			i++
		}
		defaultConfigs := NewDefaultTestConfigs(clusterNames)
		s, as, cs := NewTestService(defaultConfigs, "localhost", tc.useTokens)

		// Define the active vars
		var activeClusters map[string]clusterInfo
		var tokens []string
		var subChanges subChanged
		var createSubs map[string]bool
		var dropSubs map[string]bool
		var grantedTokens map[tokenGrant]bool
		var revokedTokens map[string]bool

		// Setup functions
		cs.QueryFunc = func(clusterName string, q influxcli.Query) (*influxcli.Response, error) {
			log.Println("query:", q.Command)
			switch {
			case q.Command == "SHOW DATABASES":
				dbs := make([][]interface{}, 0, len(activeClusters[clusterName].dbrps))
				for db := range activeClusters[clusterName].dbrps {
					dbs = append(dbs, []interface{}{db})
				}
				return &influxcli.Response{
					Results: []influxcli.Result{{
						Series: []models.Row{
							{
								Values: dbs,
							},
						},
					}},
				}, nil
			case strings.HasPrefix(q.Command, "SHOW RETENTION POLICIES ON"):
				stmt, _ := influxql.ParseStatement(q.Command)
				if show, ok := stmt.(*influxql.ShowRetentionPoliciesStatement); ok {
					var rps [][]interface{}
					for _, rp := range activeClusters[clusterName].dbrps[show.Database] {
						rps = append(rps, []interface{}{rp})
					}
					return &influxcli.Response{
						Results: []influxcli.Result{{
							Series: []models.Row{
								{
									Values: rps,
								},
							},
						}},
					}, nil
				}
				return nil, fmt.Errorf("invalid show rp query: %s", q.Command)
			case q.Command == "SHOW SUBSCRIPTIONS":
				result := influxcli.Result{}
				for db, subs := range activeClusters[clusterName].subs {
					series := models.Row{
						Name: db,
						Columns: []string{
							"name",
							"retention_policy",
							"mode",
							"destinations",
						},
					}
					for _, rp := range subs {
						var destinations []interface{}
						switch {
						case subChanges.NoDests:
							destinations = nil
						case subChanges.InvalidURL:
							destinations = []interface{}{"://broken url"}
						case subChanges.WrongProtocol:
							destinations = []interface{}{"unknown://localhost:9092"}
						case subChanges.Host:
							destinations = []interface{}{"http://example.com:9092"}
						case subChanges.Port:
							destinations = []interface{}{"http://localhost:666"}
						case subChanges.NoUser:
							destinations = []interface{}{"http://localhost:9092"}
						case subChanges.ExtraUser:
							destinations = []interface{}{"http://bob@localhost:9092"}
						case subChanges.WrongUser:
							destinations = []interface{}{"http://bob:tokendata@localhost:9092"}
						case subChanges.NoPassword:
							destinations = []interface{}{"http://~subscriber@localhost:9092"}
						case subChanges.WrongCluster:
							destinations = testDestinationsWithTokensForCluster("wrong")
						default:
							if tc.useTokens {
								destinations = testDestinationsWithTokensForCluster(clusterName)
							} else {
								destinations = []interface{}{testDestination}
							}
						}
						series.Values = append(series.Values, []interface{}{
							testSubName,
							rp,
							subMode,
							destinations,
						})
					}
					result.Series = append(result.Series, series)
				}
				return &influxcli.Response{Results: []influxcli.Result{result}}, nil
			case strings.HasPrefix(q.Command, "CREATE SUBSCRIPTION"):
				createSubs[q.Command] = true
				return &influxcli.Response{}, nil
			case strings.HasPrefix(q.Command, "DROP SUBSCRIPTION"):
				dropSubs[q.Command] = true
				return &influxcli.Response{}, nil
			default:
				msg := fmt.Sprintf("unexpected query: %s", q.Command)
				t.Error(msg)
				return nil, errors.New(msg)
			}
		}
		as.ListSubscriptionTokensFunc = func() ([]string, error) {
			ts := make([]string, len(tokens))
			for i, token := range tokens {
				ts[i] = base64.RawURLEncoding.EncodeToString([]byte(token))
			}
			return ts, nil
		}
		as.GrantSubscriptionAccessFunc = func(token, db, rp string) error {
			raw, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				return err
			}
			log.Println("granted token:", string(raw), db, rp)
			grantedTokens[tokenGrant{
				token: string(raw),
				db:    db,
				rp:    rp,
			}] = true
			return nil
		}
		as.RevokeSubscriptionAccessFunc = func(token string) error {
			raw, err := base64.RawURLEncoding.DecodeString(token)
			if err != nil {
				return err
			}
			log.Println("revoked token:", string(raw))
			revokedTokens[string(raw)] = true
			return nil
		}

		// Run first round
		activeClusters = tc.clusters
		tokens = tc.tokens
		subChanges = tc.subChanged
		createSubs = make(map[string]bool)
		dropSubs = make(map[string]bool)
		grantedTokens = make(map[tokenGrant]bool)
		revokedTokens = make(map[string]bool)

		log.Println("D! first round")
		if err := s.Open(); err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		validate(
			t,
			testName+"-1",
			tc.createSubs,
			tc.dropSubs,
			tc.grantedTokens,
			tc.revokedTokens,
			createSubs,
			dropSubs,
			grantedTokens,
			revokedTokens,
		)

		// Run second round
		activeClusters = tc.secondClusters
		tokens = tc.secondTokens
		subChanges = tc.secondSubChanged
		createSubs = make(map[string]bool)
		dropSubs = make(map[string]bool)
		grantedTokens = make(map[tokenGrant]bool)
		revokedTokens = make(map[string]bool)

		log.Println("D! second round")
		if len(tc.partialConfigs) > 0 {
			configs := make([]interface{}, 0, len(tc.partialConfigs))
			for name, pc := range tc.partialConfigs {
				c := defaultConfigs[clusterNameLookup[name]]
				c.Subscriptions = pc.configSubs
				c.ExcludedSubscriptions = pc.configExSubs
				configs = append(configs, c)
			}
			if err := s.Update(configs); err != nil {
				t.Fatal(err)
			}
		}
		s.LinkSubscriptions()

		validate(
			t,
			testName+"-2",
			tc.secondCreateSubs,
			tc.secondDropSubs,
			tc.secondGrantedTokens,
			tc.secondRevokedTokens,
			createSubs,
			dropSubs,
			grantedTokens,
			revokedTokens,
		)
	}
}

func validate(
	t *testing.T,
	testName string,
	expCreateSubs []string,
	expDropSubs []string,
	expGrantedTokens []tokenGrant,
	expRevokedTokens []string,
	gotCreateSubs map[string]bool,
	gotDropSubs map[string]bool,
	gotGrantedTokens map[tokenGrant]bool,
	gotRevokedTokens map[string]bool,
) {
	// Validate create subs queries
	expCreateSubsMap := make(map[string]bool, len(expCreateSubs))
	for _, createSub := range expCreateSubs {
		expCreateSubsMap[createSub] = true
	}
	if got, exp := len(gotCreateSubs), len(expCreateSubs); exp != got {
		t.Errorf("[%s] unexpected count of create sub queries: got %d exp %d", testName, got, exp)
	}
	for q := range expCreateSubsMap {
		if !gotCreateSubs[q] {
			t.Errorf("[%s] missing create sub query: %s", testName, q)
		}
	}
	for q := range gotCreateSubs {
		if !expCreateSubsMap[q] {
			t.Errorf("[%s] extra create sub query: %s", testName, q)
		}
	}

	// Validate drop subs queries
	expDropSubsMap := make(map[string]bool, len(expDropSubs))
	for _, dropSub := range expDropSubs {
		expDropSubsMap[dropSub] = true
	}
	if got, exp := len(gotDropSubs), len(expDropSubs); exp != got {
		t.Errorf("[%s] unexpected count of drop sub queries: got %d exp %d", testName, got, exp)
	}
	for q := range expDropSubsMap {
		if !gotDropSubs[q] {
			t.Errorf("[%s] missing drop sub query: %s", testName, q)
		}
	}
	for q := range gotDropSubs {
		if !expDropSubsMap[q] {
			t.Errorf("[%s] extra drop sub query: %s", testName, q)
		}
	}

	// Validate granted tokens
	expGrantedTokensMap := make(map[tokenGrant]bool, len(expGrantedTokens))
	for _, grantedToken := range expGrantedTokens {
		expGrantedTokensMap[grantedToken] = true
	}
	if got, exp := len(gotGrantedTokens), len(expGrantedTokens); exp != got {
		t.Errorf("[%s] unexpected count of granted tokens: got %d exp %d", testName, got, exp)
	}
	for token := range expGrantedTokensMap {
		if !gotGrantedTokens[token] {
			t.Errorf("[%s] missing granted token: %v", testName, token)
		}
	}
	for token := range gotGrantedTokens {
		if !expGrantedTokensMap[token] {
			t.Errorf("[%s] extra granted token: %v", testName, token)
		}
	}

	// Validate revoked tokens
	expRevokedTokensMap := make(map[string]bool, len(expRevokedTokens))
	for _, revokedToken := range expRevokedTokens {
		expRevokedTokensMap[revokedToken] = true
	}
	if got, exp := len(gotRevokedTokens), len(expRevokedTokens); exp != got {
		t.Errorf("[%s] unexpected count of revoked tokens: got %d exp %d", testName, got, exp)
	}
	for token := range expRevokedTokensMap {
		if !gotRevokedTokens[token] {
			t.Errorf("[%s] missing revoked token: %s", testName, token)
		}
	}
	for token := range gotRevokedTokens {
		if !expRevokedTokensMap[token] {
			t.Errorf("[%s] extra revoked token: %s", testName, token)
		}
	}
}

func NewDefaultTestConfigs(clusters []string) []influxdb.Config {
	if len(clusters) == 0 {
		clusters = []string{testClusterName}
	}
	configs := make([]influxdb.Config, len(clusters))
	for i, name := range clusters {
		configs[i] = influxdb.Config{
			Enabled:              true,
			Name:                 name,
			URLs:                 []string{fmt.Sprintf("http://%s", name)},
			Default:              true,
			SubscriptionProtocol: "http",
			StartUpTimeout:       0,
			// Do not start syncing goroutines
			SubscriptionSyncInterval: 0,
		}
		if err := configs[i].Validate(); err != nil {
			panic(err)
		}
	}
	return configs
}

func NewTestService(configs []influxdb.Config, hostname string, useTokens bool) (*influxdb.Service, *authService, *clientCreator) {
	httpPort := 9092
	d := diagService.NewInfluxDBHandler()
	s, err := influxdb.NewService(
		configs,
		httpPort,
		hostname,
		ider{clusterID: testKapacitorClusterID, serverID: uuid.New()},
		useTokens, d)
	if err != nil {
		panic(err)
	}
	s.HTTPDService = httpdService{}
	as := &authService{}
	s.AuthService = as
	cs := &clientCreator{}
	s.ClientCreator = cs
	s.RandReader = randReader{}
	return s, as, cs
}

type authService struct {
	GrantSubscriptionAccessFunc  func(token, db, rp string) error
	ListSubscriptionTokensFunc   func() ([]string, error)
	RevokeSubscriptionAccessFunc func(token string) error
}

func (a *authService) GrantSubscriptionAccess(token, db, rp string) error {
	if a.GrantSubscriptionAccessFunc != nil {
		return a.GrantSubscriptionAccessFunc(token, db, rp)
	}
	return nil
}
func (a *authService) ListSubscriptionTokens() ([]string, error) {
	if a.ListSubscriptionTokensFunc != nil {
		return a.ListSubscriptionTokensFunc()
	}
	return nil, nil
}
func (a *authService) RevokeSubscriptionAccess(token string) error {
	if a.RevokeSubscriptionAccessFunc != nil {
		return a.RevokeSubscriptionAccessFunc(token)
	}
	return nil
}

type clientCreator struct {
	// Index for the order the client was created, matches the order clusters are created.
	CreateFunc func(influxcli.Config) (influxcli.ClientUpdater, error)

	// Cient functions passed down to any created client
	PingFunc   func(ctx context.Context) (time.Duration, string, error)
	WriteFunc  func(bp influxcli.BatchPoints) error
	QueryFunc  func(clusterName string, q influxcli.Query) (*influxcli.Response, error)
	UpdateFunc func(influxcli.Config) error
}

func (c *clientCreator) Create(config influxcli.Config) (influxcli.ClientUpdater, error) {
	if c.CreateFunc != nil {
		return c.CreateFunc(config)
	}
	// Retrieve cluster name from URL
	u, _ := url.Parse(config.URLs[0])
	cli := influxDBClient{
		clusterName: u.Host,
		PingFunc:    c.PingFunc,
		WriteFunc:   c.WriteFunc,
		QueryFunc:   c.QueryFunc,
		UpdateFunc:  c.UpdateFunc,
	}
	return cli, nil
}

type influxDBClient struct {
	clusterName string
	PingFunc    func(ctx context.Context) (time.Duration, string, error)
	WriteFunc   func(bp influxcli.BatchPoints) error
	QueryFunc   func(clusterName string, q influxcli.Query) (*influxcli.Response, error)
	UpdateFunc  func(influxcli.Config) error
}

func (c influxDBClient) Close() error {
	return nil
}

func (c influxDBClient) Ping(ctx context.Context) (time.Duration, string, error) {
	if c.PingFunc != nil {
		return c.PingFunc(ctx)
	}
	return 0, "testversion", nil
}
func (c influxDBClient) Write(bp influxcli.BatchPoints) error {
	if c.WriteFunc != nil {
		return c.WriteFunc(bp)
	}
	return nil
}
func (c influxDBClient) Query(q influxcli.Query) (*influxcli.Response, error) {
	if c.QueryFunc != nil {
		return c.QueryFunc(c.clusterName, q)
	}
	return &influxcli.Response{}, nil
}
func (c influxDBClient) Update(config influxcli.Config) error {
	if c.UpdateFunc != nil {
		return c.UpdateFunc(config)
	}
	return nil
}

type logSerivce struct {
}

func (logSerivce) NewLogger(p string, flags int) *log.Logger {
	return log.New(os.Stderr, p, flags)
}

type httpdService struct {
}

func (httpdService) AddRoutes([]httpd.Route) error {
	return nil
}
func (httpdService) DelRoutes([]httpd.Route) {}

type randReader struct {
}

func (randReader) Read(p []byte) (int, error) {
	return copy(p, []byte(randomTokenData)), nil
}

type ider struct {
	clusterID,
	serverID uuid.UUID
}

func (i ider) ClusterID() uuid.UUID {
	return i.clusterID
}
func (i ider) ServerID() uuid.UUID {
	return i.serverID
}
