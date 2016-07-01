package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/coreos/pkg/flagutil"
	"github.com/golang/glog"
	consulapi "github.com/hashicorp/consul/api"

	kapi "k8s.io/kubernetes/pkg/api"
	kcache "k8s.io/kubernetes/pkg/client/cache"
)

var (
	opts cliOpts
	wg   sync.WaitGroup
)

const (
	consulTag          = "kube2consul"
	kube2consulVersion = "v1.0.0"
)

type kube2consul struct {
	consulCatalog  *consulapi.Catalog
	endpointsStore kcache.Store
}

type cliOpts struct {
	kubeAPI      string
	consulAPI    string
	consulToken  string
	resyncPeriod int
	version      bool
	kubeInsecure bool
}

func init() {
	flag.BoolVar(&opts.version, "version", false, "Prints kube2consul version")
	flag.IntVar(&opts.resyncPeriod, "resync-period", 30, "Resynchronization period in second")
	flag.StringVar(&opts.kubeAPI, "kubernetes-api", "", "Kubernetes API URL")
	flag.BoolVar(&opts.kubeInsecure, "kubernetes-insecure", false, "Ignore HTTPS certificate warnings")
	flag.StringVar(&opts.consulAPI, "consul-api", "127.0.0.1:8500", "Consul API URL")
	flag.StringVar(&opts.consulToken, "consul-token", "", "Consul API token")
}

func inSlice(value string, slice []string) bool {
	for _, s := range slice {
		if s == value {
			return true
		}
	}
	return false
}

func (k2c *kube2consul) RemoveDNSGarbage() {
	epSet := make(map[string]struct{})

	for _, obj := range k2c.endpointsStore.List() {
		if ep, ok := obj.(*kapi.Endpoints); ok {
			epSet[ep.Name] = struct{}{}
		}
	}

	services, _, err := k2c.consulCatalog.Services(nil)
	if err != nil {
		glog.Errorf("Cannot remove DNS garbage: %v", err)
		return
	}

	for name, tags := range services {
		if !inSlice(consulTag, tags) {
			continue
		}

		if _, ok := epSet[name]; !ok {
			k2c.removeDeletedEndpoints(name, []Endpoint{})
		}
	}
}

func main() {
	// parse flags
	flag.Parse()
	flagutil.SetFlagsFromEnv(flag.CommandLine, "K2C")

	if opts.version {
		fmt.Println(kube2consulVersion)
		os.Exit(0)
	}

	// create consul client
	consulClient, err := newConsulClient(opts.consulAPI, opts.consulToken)
	if err != nil {
		glog.Fatalf("Failed to create a consul client: %v", err)
	}

	// create kubernetes client
	kubeClient, err := newKubeClient(opts.kubeAPI, opts.kubeInsecure)
	if err != nil {
		glog.Fatalf("Failed to create a kubernetes client: %v", err)
	}

	k2c := kube2consul{
		consulCatalog: consulClient.Catalog(),
	}

	k2c.endpointsStore = k2c.watchEndpoints(kubeClient)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-time.NewTicker(time.Duration(opts.resyncPeriod) * time.Second).C:
				k2c.RemoveDNSGarbage()
			}
		}
	}()

	wg.Wait()
}
