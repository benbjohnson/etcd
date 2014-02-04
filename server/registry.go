package server

import (
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/coreos/etcd/log"
	"github.com/coreos/etcd/store"
)

// The location of the peer URL data.
const RegistryPeerKey = "/_etcd/machines"

// The location of the proxy URL data.
const RegistryProxyKey = "/_etcd/proxies"

// The Registry stores URL information for nodes.
type Registry struct {
	sync.Mutex
	store store.Store
	peers map[string]*node
	proxies map[string]*node
}

// The internal storage format of the registry.
type node struct {
	peerVersion string
	peerURL     string
	url         string
}

// Creates a new Registry.
func NewRegistry(s store.Store) *Registry {
	r := &Registry{store: s}
	r.peers = r.load(RegistryPeerKey)
	r.proxies = r.load(RegistryProxyKey)
	return r
}

// Adds a peer to the registry.
func (r *Registry) RegisterPeer(name string, peerURL string, machURL string) error {
	if err := r.register(RegistryPeerKey, name, peerURL, machURL); err != nil {
		return err
	}
	r.peers = r.load(RegistryPeerKey)
	return nil
}

// Adds a proxy to the registry.
func (r *Registry) RegisterProxy(name string, peerURL string, machURL string) error {
	if err := r.register(RegistryProxyKey, name, peerURL, machURL); err != nil {
		return err
	}
	r.proxies = r.load(RegistryProxyKey)
	return nil
}

// Adds a node to the registry.
func (r *Registry) register(key string, name string, peerURL string, machURL string) error {
	r.Lock()
	defer r.Unlock()

	// TODO(benbjohnson): Disallow registration in peers and proxies at the same time.

	// Write data to store.
	v := url.Values{}
	v.Set("raft", peerURL)
	v.Set("etcd", machURL)
	_, err := r.store.Create(path.Join(key, name), false, v.Encode(), false, store.Permanent)
	log.Debugf("Register: %s", name)
	return err
}

// Removes a node from the registry.
func (r *Registry) Unregister(name string) error {
	if r.proxies[name] != nil {
		return r.unregister(RegistryProxyKey, name)
	}
	return r.unregister(RegistryPeerKey, name)
}

// Removes a node from the registry.
func (r *Registry) unregister(key string, name string) error {
	r.Lock()
	defer r.Unlock()

	// Remove from cache.
	// delete(r.nodes, name)

	// Remove the key from the store.
	_, err := r.store.Delete(path.Join(key, name), false, false)
	log.Debugf("Unregister: %s", name)
	return err
}

// Returns the number of peers in the cluster.
func (r *Registry) PeerCount() int {
	return r.count(RegistryPeerKey)
}

// Returns the number of proxies in the cluster.
func (r *Registry) ProxyCount() int {
	return r.count(RegistryProxyKey)
}

// Returns the number of nodes in the cluster.
func (r *Registry) count(key string) int {
	e, err := r.store.Get(key, false, false)
	if err != nil {
		return 0
	}
	return len(e.Node.Nodes)
}

// Retrieves the client URL for a given node by name.
func (r *Registry) ClientURL(name string) (string, bool) {
	r.Lock()
	defer r.Unlock()
	return r.clientURL(name)
}

func (r *Registry) clientURL(name string) (string, bool) {
	if n := r.peers[name]; n != nil {
		return n.url, true
	}
	if n := r.proxies[name]; n != nil {
		return n.url, true
	}
	return "", false
}

// Retrieves the peer URL for a given node by name.
func (r *Registry) PeerURL(name string) (string, bool) {
	r.Lock()
	defer r.Unlock()
	return r.peerURL(name)
}

func (r *Registry) peerURL(name string) (string, bool) {
	if n := r.peers[name]; n != nil {
		return n.peerURL, true
	}
	if n := r.proxies[name]; n != nil {
		return n.peerURL, true
	}
	return "", false
}

// Retrieves the Client URLs for all peers.
func (r *Registry) PeerClientURLs(leaderName, selfName string) []string {
	return r.urls(RegistryPeerKey, leaderName, selfName, r.clientURL)
}

// Retrieves the Peer URLs for all peers.
func (r *Registry) PeerPeerURLs(leaderName, selfName string) []string {
	return r.urls(RegistryPeerKey, leaderName, selfName, r.peerURL)
}

// Retrieves the Client URLs for all proxies.
func (r *Registry) ProxyClientURLs(leaderName, selfName string) []string {
	return r.urls(RegistryProxyKey, leaderName, selfName, r.clientURL)
}

// Retrieves the Proxy URLs for all proxies.
func (r *Registry) ProxyPeerURLs(leaderName, selfName string) []string {
	return r.urls(RegistryProxyKey, leaderName, selfName, r.peerURL)
}

// Retrieves the URLs for all nodes using url function.
func (r *Registry) urls(key, leaderName, selfName string, url func(name string) (string, bool)) []string {
	r.Lock()
	defer r.Unlock()

	// Build list including the leader and self.
	urls := make([]string, 0)
	if url, _ := url(leaderName); len(url) > 0 {
		urls = append(urls, url)
	}

	// Retrieve a list of all nodes.
	if e, _ := r.store.Get(key, false, false); e != nil {
		// Lookup the URL for each one.
		for _, pair := range e.Node.Nodes {
			_, name := filepath.Split(pair.Key)
			if url, _ := url(name); len(url) > 0 && name != leaderName {
				urls = append(urls, url)
			}
		}
	}

	log.Infof("URLs: %s / %s (%s)", leaderName, selfName, strings.Join(urls, ","))

	return urls
}

// Removes a node from the cache.
func (r *Registry) Invalidate(name string) {
	delete(r.peers, name)
	delete(r.proxies, name)
}

func (r *Registry) load(key string) map[string]*node {
	nodes := make(map[string]*node)

	// Retrieve from store.
	e, err := r.store.Get(key, false, false)
	if e == nil || err != nil {
		return nodes
	}

	// Loop over collection and create nodes.
	for _, n := range e.Node.Nodes {
		name := path.Base(n.Key)
		m, err := url.ParseQuery(n.Value)
		if err != nil {
			panic(fmt.Sprintf("Failed to parse registry entry: %s: %s", key, name))
		}

		nodes[name] = &node{
			url:     m["etcd"][0],
			peerURL: m["raft"][0],
		}
	}

	return nodes
}
