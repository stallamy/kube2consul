package main

import (
	"github.com/golang/glog"
	consulapi "github.com/hashicorp/consul/api"
	kapi "k8s.io/kubernetes/pkg/api"
)

func newConsulClient(consulAPI, consulToken string) (*consulapi.Client, error) {
	config := consulapi.DefaultConfig()
	config.Address = consulAPI
	config.Token = consulToken

	return consulapi.NewClient(config)
}

func (k2c *kube2consul) addPending(e Endpoint) {
    k2c.lock.Lock()
    defer k2c.lock.Unlock()
    k2c.pending[e.RefName] = e
}

func (k2c *kube2consul) removePending(e Endpoint) {
    k2c.lock.Lock()
    defer k2c.lock.Unlock()
    delete(k2c.pending, e.RefName)
}

func (k2c *kube2consul) registerPod(p *kapi.Pod) {
	if p.ObjectMeta.Name == "" {
		return
	}

    e, exists := k2c.pending[p.ObjectMeta.Name]
    if (exists) {
        k2c.registerEndpointPod(e,p)
        k2c.removePending(e)
    }
}

func (k2c *kube2consul) registerEndpoint(e Endpoint) {
	if e.RefName == "" {
		return
	}

	var (
        item interface{}
        exists bool
    )

    item, exists, _ = k2c.podsStore.GetByKey(e.RefName);
	if (exists) {
	    if p, ok := item.(*kapi.Pod); ok {
            k2c.registerEndpointPod(e,p)
        }
	} else {
	    k2c.addPending(e) // sometimes we see the Endpoint notification before we see the Pod definition
	}
}

func (k2c *kube2consul) registerEndpointPod(e Endpoint, p *kapi.Pod) {
    var (
        err error
        tags []string
    )
    tags = append(tags, consulTag)
    if (len(p.Annotations["tags"]) > 0) {
        tags = append(tags, p.Annotations["tags"])
    }

    service := &consulapi.AgentService{
        Service: e.Name,
        Port:    int(e.Port),
        Tags:    tags,
    }

    reg := &consulapi.CatalogRegistration{
        Node:    e.RefName,
        Address: e.Address,
        Service: service,
    }

    _, err = k2c.consulCatalog.Register(reg, nil)
    if err != nil {
        glog.Errorf("Error registrating service %v (%v, %v): %v", e.Name, e.RefName, e.Address, err)
    } else {
        glog.V(1).Infof("Update service %v (%v, %v)", e.Name, e.RefName, e.Address)
    }
}

func endpointExists(refName, address string, port int, endpoints []Endpoint) bool {
	for _, e := range endpoints {
		if e.RefName == refName && e.Address == address && int(e.Port) == port {
			return true
		}
	}
	return false
}

func (k2c *kube2consul) removeDeletedEndpoints(serviceName string, endpoints []Endpoint) {
	updatedNodes := make(map[string]struct{})
	services, _, err := k2c.consulCatalog.Service(serviceName, consulTag, nil)
	if err != nil {
		glog.Errorf("[Consul] Failed to get services: %v", err)
		return
	}

	for _, service := range services {
		if !endpointExists(service.Node, service.Address, service.ServicePort, endpoints) {
			dereg := &consulapi.CatalogDeregistration{
				Node:      service.Node,
				Address:   service.Address,
				ServiceID: service.ServiceID,
			}
			_, err := k2c.consulCatalog.Deregister(dereg, nil)
			if err != nil {
				glog.Errorf("Error deregistrating service {node: %s, service: %s, address: %s}: %v", service.Node, service.ServiceName, service.Address, err)
			} else {
				glog.Infof("Deregister service {node: %s, service: %s, address: %s}", service.Node, service.ServiceName, service.Address)
				updatedNodes[service.Node] = struct{}{}
			}
		}
	}

	// Remove all empty nodes
	for nodeName := range updatedNodes {
		if node, _, err := k2c.consulCatalog.Node(nodeName, nil); err == nil {
			if len(node.Services) == 0 {
				dereg := &consulapi.CatalogDeregistration{
					Node: nodeName,
				}
				_, err = k2c.consulCatalog.Deregister(dereg, nil)
				if err != nil {
					glog.Errorf("Error deregistrating node %s: %v", nodeName, err)
				} else {
					glog.Infof("Deregister empty node %s", nodeName)
				}

			}
		} else {
			glog.Errorf("Cannot get node %s: %v", nodeName, err)
		}
	}
}
