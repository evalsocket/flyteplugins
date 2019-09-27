// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import definition "github.com/lyft/flyteplugins/go/tasks/plugins/array/awsbatch/definition"
import mock "github.com/stretchr/testify/mock"

// Cache is an autogenerated mock type for the Cache type
type Cache struct {
	mock.Mock
}

// Get provides a mock function with given fields: key
func (_m *Cache) Get(key definition.CacheKey) (string, bool) {
	ret := _m.Called(key)

	var r0 string
	if rf, ok := ret.Get(0).(func(definition.CacheKey) string); ok {
		r0 = rf(key)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 bool
	if rf, ok := ret.Get(1).(func(definition.CacheKey) bool); ok {
		r1 = rf(key)
	} else {
		r1 = ret.Get(1).(bool)
	}

	return r0, r1
}

// Put provides a mock function with given fields: key, _a1
func (_m *Cache) Put(key definition.CacheKey, _a1 string) error {
	ret := _m.Called(key, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(definition.CacheKey, string) error); ok {
		r0 = rf(key, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}