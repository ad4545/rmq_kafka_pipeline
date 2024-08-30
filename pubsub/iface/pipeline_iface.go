package iface

import "sync"

type Pipeline interface {
	Shutdown()
	Start(*sync.WaitGroup)
	GetErrorStream() chan error
	Name() string
	IsActive() bool
	Deactivate()
}
