module github.com/influxdata/kapacitor

replace github.com/prometheus/prometheus => github.com/goller/prometheus v1.5.1-0.20170502220046-58298e738211

require (
	cloud.google.com/go v0.7.0 // indirect
	github.com/Azure/azure-sdk-for-go v0.0.0-20161028183111-bd73d950fa44 // indirect
	github.com/Azure/go-autorest v7.3.1+incompatible // indirect
	github.com/BurntSushi/toml v0.0.0-20170626110600-a368813c5e64
	github.com/PuerkitoBio/purell v1.1.0 // indirect
	github.com/PuerkitoBio/urlesc v0.0.0-20170324140228-bbf7a2afc14f // indirect
	github.com/Sirupsen/logrus v0.11.5 // indirect
	github.com/aws/aws-sdk-go v1.8.16
	github.com/benbjohnson/tmpl v0.0.0-20160209232322-8e77bc5fc079
	github.com/beorn7/perks v0.0.0-20160804104726-4c0e84591b9a // indirect
	github.com/blang/semver v3.5.0+incompatible // indirect
	github.com/bmizerany/pat v0.0.0-20170815010413-6226ea591a40 // indirect
	github.com/boltdb/bolt v1.3.0
	github.com/cenkalti/backoff v1.0.0
	github.com/cespare/xxhash v1.0.0 // indirect
	github.com/coreos/go-oidc v0.0.0-20170307191026-be73733bb8cc // indirect
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf // indirect
	github.com/davecgh/go-spew v1.1.0
	github.com/dgrijalva/jwt-go v3.0.0+incompatible
	github.com/dgryski/go-bits v0.0.0-20180113010104-bd8a69a71dc2 // indirect
	github.com/dgryski/go-bitstream v0.0.0-20180413035011-3522498ce2c8 // indirect
	github.com/docker/distribution v2.6.1+incompatible // indirect
	github.com/docker/docker v1.13.1
	github.com/docker/go-connections v0.2.1 // indirect
	github.com/docker/go-units v0.3.1 // indirect
	github.com/dustin/go-humanize v0.0.0-20170228161531-259d2a102b87
	github.com/eclipse/paho.mqtt.golang v1.0.0
	github.com/emicklei/go-restful v0.0.0-20151126145626-777bb3f19bca // indirect
	github.com/evanphx/json-patch v0.0.0-20160803213441-30afec6a1650
	github.com/geoffgarside/ber v0.0.0-20170306085127-854377f11dfb // indirect
	github.com/ghodss/yaml v0.0.0-20161207003320-04f313413ffd
	github.com/go-ini/ini v1.27.0 // indirect
	github.com/go-kit/kit v0.7.0 // indirect
	github.com/go-logfmt/logfmt v0.3.0 // indirect
	github.com/go-openapi/jsonpointer v0.0.0-20170102174223-779f45308c19 // indirect
	github.com/go-openapi/jsonreference v0.0.0-20161105162150-36d33bfe519e // indirect
	github.com/go-openapi/spec v0.0.0-20170413060731-e51c28f07047 // indirect
	github.com/go-openapi/swag v0.0.0-20170424051500-24ebf76d720b // indirect
	github.com/go-stack/stack v1.8.0 // indirect
	github.com/gogo/protobuf v0.0.0-20170307180453-100ba4e88506 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/protobuf v0.0.0-20170331031902-2bba0603135d
	github.com/golang/snappy v0.0.0-20170215233205-553a64147049 // indirect
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c // indirect
	github.com/google/go-cmp v0.1.0
	github.com/google/gofuzz v0.0.0-20161122191042-44d81051d367 // indirect
	github.com/google/uuid v0.0.0-20170306145142-6a5e28554805
	github.com/googleapis/gax-go v0.0.0-20170321005343-9af46dd5a171 // indirect
	github.com/googleapis/gnostic v0.2.0 // indirect
	github.com/gophercloud/gophercloud v0.0.0-20180905015028-d629ff82f917 // indirect
	github.com/gorhill/cronexpr v0.0.0-20140423231348-a557574d6c02
	github.com/gregjones/httpcache v0.0.0-20180305231024-9cad4c3443a7 // indirect
	github.com/hashicorp/consul v0.8.1 // indirect
	github.com/hashicorp/go-cleanhttp v0.0.0-20170211013415-3573b8b52aa7 // indirect
	github.com/hashicorp/go-rootcerts v0.0.0-20160503143440-6bb64b370b90 // indirect
	github.com/hashicorp/golang-lru v0.5.0 // indirect
	github.com/hashicorp/serf v0.8.1 // indirect
	github.com/influxdata/influxdb v1.1.4
	github.com/influxdata/usage-client v0.0.0-20160829180054-6d3895376368
	github.com/influxdata/wlog v0.0.0-20160411224016-7c63b0a71ef8
	github.com/jmespath/go-jmespath v0.0.0-20151117175822-3433f3ea46d9 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/json-iterator/go v1.1.5 // indirect
	github.com/juju/ratelimit v0.0.0-20170314011755-acf38b000a03 // indirect
	github.com/jwilder/encoding v0.0.0-20170811194829-b4e1701a28ef // indirect
	github.com/k-sone/snmpgo v3.2.0+incompatible
	github.com/kimor79/gollectd v1.0.0 // indirect
	github.com/mailru/easyjson v0.0.0-20180606163543-3fdea8d05856
	github.com/mattn/go-runewidth v0.0.2 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.0 // indirect
	github.com/miekg/dns v0.0.0-20170412184748-6ebcb714d369 // indirect
	github.com/mitchellh/copystructure v0.0.0-20170116004449-f81071c9d77b
	github.com/mitchellh/go-homedir v0.0.0-20161203194507-b8bc1bf76747 // indirect
	github.com/mitchellh/mapstructure v0.0.0-20161204053518-5a0325d7fafa
	github.com/mitchellh/reflectwalk v0.0.0-20170110165207-417edcfd99a4
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20161129095857-cc309e4a2223 // indirect
	github.com/pborman/uuid v0.0.0-20160209185913-a97ce2ca70fa // indirect
	github.com/peterbourgon/diskv v2.0.1+incompatible // indirect
	github.com/pkg/errors v0.8.0
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v0.8.0 // indirect
	github.com/prometheus/client_model v0.0.0-20170216185247-6f3806018612 // indirect
	github.com/prometheus/common v0.0.0-20170418155210-9e0844febd9e
	github.com/prometheus/procfs v0.0.0-20170424204552-6ac8c5d890d4 // indirect
	github.com/prometheus/prometheus v2.3.2+incompatible
	github.com/rakyll/statik v0.1.4 // indirect
	github.com/russross/blackfriday v0.0.0-20151110051855-0b647d0506a6 // indirect
	github.com/samuel/go-zookeeper v0.0.0-20161028232340-1d7be4effb13 // indirect
	github.com/segmentio/kafka-go v0.0.0-20180320154959-140b1a903e14
	github.com/serenize/snaker v0.0.0-20161123064335-543781d2b79b
	github.com/shurcooL/go v0.0.0-20170331015642-20b4b0a35211 // indirect
	github.com/shurcooL/markdownfmt v0.0.0-20170214213350-10aae0a270ab
	github.com/shurcooL/sanitized_anchor_name v0.0.0-20160918041101-1dba4b3954bc // indirect
	github.com/spf13/pflag v0.0.0-20170418052314-2300d0f8576f // indirect
	github.com/stretchr/testify v1.1.4
	github.com/syndtr/goleveldb v0.0.0-20170409015612-8c81ea47d4c4 // indirect
	github.com/ugorji/go v0.0.0-20170312112114-708a42d24682 // indirect
	golang.org/x/crypto v0.0.0-20170420163513-0242f07995e6 // indirect
	golang.org/x/net v0.0.0-20170423033148-d212a1ef2de2 // indirect
	golang.org/x/oauth2 v0.0.0-20170412232759-a6bd8cefa181 // indirect
	golang.org/x/sys v0.0.0-20170407050850-f3918c30c5c2 // indirect
	golang.org/x/text v0.0.0-20170422073719-a9a820217f98 // indirect
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2 // indirect
	google.golang.org/api v0.0.0-20170421051952-fbbaff182731 // indirect
	google.golang.org/appengine v1.0.0 // indirect
	google.golang.org/grpc v1.2.1 // indirect
	gopkg.in/alexcesaro/quotedprintable.v3 v3.0.0-20150716171945-2caba252f4dc // indirect
	gopkg.in/fsnotify.v1 v1.4.2 // indirect
	gopkg.in/fsnotify/fsnotify.v1 v1.4.7 // indirect
	gopkg.in/gomail.v2 v2.0.0-20150902115704-41f357289737
	gopkg.in/inf.v0 v0.9.0 // indirect
	gopkg.in/yaml.v2 v2.0.0-20160928153709-a5b47d31c556 // indirect
	k8s.io/api v0.0.0-20180904190239-89dfcd0b1128 // indirect
	k8s.io/apimachinery v0.0.0-20180904031649-6429050ef506 // indirect
	k8s.io/client-go v1.5.1 // indirect
)
