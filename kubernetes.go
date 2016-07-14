package main

import (
	"time"

	"github.com/golang/glog"

	kapi "k8s.io/kubernetes/pkg/api"
	"k8s.io/kubernetes/pkg/api/unversioned"
	kcache "k8s.io/kubernetes/pkg/client/cache"
	"k8s.io/kubernetes/pkg/client/restclient"
	kclient "k8s.io/kubernetes/pkg/client/unversioned"
	kframework "k8s.io/kubernetes/pkg/controller/framework"
	kselector "k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/util/wait"
)

// Returns a cache.ListWatch that gets all changes to endpoints.
func createEndpointsLW(kubeClient *kclient.Client) *kcache.ListWatch {
	return kcache.NewListWatchFromClient(kubeClient, "endpoints", kapi.NamespaceAll, kselector.Everything())
}

// Returns a cache.ListWatch that gets all changes to pods.
func createEndpointsPOD(kubeClient *kclient.Client) *kcache.ListWatch {
	return kcache.NewListWatchFromClient(kubeClient, "pods", "cargo", kselector.Everything())
}

func newKubeClient(kubeAPI string,kubeInsecure bool,kubeToken string) (*kclient.Client, error) {
    if (kubeAPI == "") {
        return kclient.NewInCluster()
    }

	var (
		config *restclient.Config
	)

	config = &restclient.Config{
		Host:          kubeAPI,
		ContentConfig: restclient.ContentConfig{GroupVersion: &unversioned.GroupVersion{Version: "v1"}},
        Insecure:      kubeInsecure,
        BearerToken:   kubeToken,
	}

	glog.Infof("Using %s for kubernetes master", config.Host)
	glog.Infof("Using kubernetes API %v", config.GroupVersion)
	return kclient.New(config)
}

func (k2c *kube2consul) handleEndpointUpdate(obj interface{}) {
	if e, ok := obj.(*kapi.Endpoints); ok {
		k2c.updateEndpoints(e)
	}
}

func (k2c *kube2consul) handlePodUpdate(obj interface{}) {
	if p, ok := obj.(*kapi.Pod); ok {
	    k2c.registerPod(p)
	}
}

func (k2c *kube2consul) watchEndpoints(kubeClient *kclient.Client) kcache.Store {
	eStore, eController := kframework.NewInformer(
		createEndpointsLW(kubeClient),
		&kapi.Endpoints{},
		time.Duration(opts.resyncPeriod)*time.Second,
		kframework.ResourceEventHandlerFuncs{
			AddFunc: func(newObj interface{}) {
				go k2c.handleEndpointUpdate(newObj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				go k2c.handleEndpointUpdate(newObj)
			},
		},
	)

	go eController.Run(wait.NeverStop)
	return eStore
}

func (k2c *kube2consul) watchPods(kubeClient *kclient.Client) kcache.Store {
	eStore, eController := kframework.NewInformer(
		createEndpointsPOD(kubeClient),
		&kapi.Pod{},
		time.Duration(opts.resyncPeriod)*time.Second,
		kframework.ResourceEventHandlerFuncs{
			AddFunc: func(newObj interface{}) {
				go k2c.handlePodUpdate(newObj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				go k2c.handlePodUpdate(newObj)
			},
		},
	)

	go eController.Run(wait.NeverStop)
	return eStore
}