package util

// ResourceHolder holds resources and returns a release func so that caller can call this to relese
// the resource.
type ResourceHolder func(r interface{}) Releaser

// Releaser releases the resource.
type Releaser func()
