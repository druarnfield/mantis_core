package driver

import (
	"fmt"
	"sync"
)

// Registry manages available database drivers.
type Registry struct {
	mu      sync.RWMutex
	drivers map[string]Driver
}

// NewRegistry creates a new empty driver registry.
func NewRegistry() *Registry {
	return &Registry{
		drivers: make(map[string]Driver),
	}
}

// Register adds a driver to the registry.
// If a driver with the same name already exists, it will be replaced.
func (r *Registry) Register(d Driver) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drivers[d.Name()] = d
}

// Get retrieves a driver by name.
// Returns an error if the driver is not found.
func (r *Registry) Get(name string) (Driver, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d, ok := r.drivers[name]
	if !ok {
		return nil, fmt.Errorf("driver not found: %s", name)
	}
	return d, nil
}

// Has checks if a driver with the given name is registered.
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.drivers[name]
	return ok
}

// Names returns a list of all registered driver names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.drivers))
	for name := range r.drivers {
		names = append(names, name)
	}
	return names
}

// DefaultRegistry is the global driver registry.
var DefaultRegistry = NewRegistry()

// Register adds a driver to the default registry.
func Register(d Driver) {
	DefaultRegistry.Register(d)
}

// Get retrieves a driver from the default registry.
func Get(name string) (Driver, error) {
	return DefaultRegistry.Get(name)
}

// Has checks if a driver is registered in the default registry.
func Has(name string) bool {
	return DefaultRegistry.Has(name)
}
