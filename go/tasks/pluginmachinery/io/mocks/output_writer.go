// Code generated by mockery v1.0.1. DO NOT EDIT.

package mocks

import context "context"
import io "github.com/lyft/flyteplugins/go/tasks/pluginmachinery/io"
import mock "github.com/stretchr/testify/mock"
import storage "github.com/lyft/flytestdlib/storage"

// OutputWriter is an autogenerated mock type for the OutputWriter type
type OutputWriter struct {
	mock.Mock
}

type OutputWriter_GetErrorPath struct {
	*mock.Call
}

func (_m OutputWriter_GetErrorPath) Return(_a0 storage.DataReference) *OutputWriter_GetErrorPath {
	return &OutputWriter_GetErrorPath{Call: _m.Call.Return(_a0)}
}

func (_m *OutputWriter) OnGetErrorPath() *OutputWriter_GetErrorPath {
	c := _m.On("GetErrorPath")
	return &OutputWriter_GetErrorPath{Call: c}
}

func (_m *OutputWriter) OnGetErrorPathMatch(matchers ...interface{}) *OutputWriter_GetErrorPath {
	c := _m.On("GetErrorPath", matchers...)
	return &OutputWriter_GetErrorPath{Call: c}
}

// GetErrorPath provides a mock function with given fields:
func (_m *OutputWriter) GetErrorPath() storage.DataReference {
	ret := _m.Called()

	var r0 storage.DataReference
	if rf, ok := ret.Get(0).(func() storage.DataReference); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(storage.DataReference)
	}

	return r0
}

type OutputWriter_GetOutputPath struct {
	*mock.Call
}

func (_m OutputWriter_GetOutputPath) Return(_a0 storage.DataReference) *OutputWriter_GetOutputPath {
	return &OutputWriter_GetOutputPath{Call: _m.Call.Return(_a0)}
}

func (_m *OutputWriter) OnGetOutputPath() *OutputWriter_GetOutputPath {
	c := _m.On("GetOutputPath")
	return &OutputWriter_GetOutputPath{Call: c}
}

func (_m *OutputWriter) OnGetOutputPathMatch(matchers ...interface{}) *OutputWriter_GetOutputPath {
	c := _m.On("GetOutputPath", matchers...)
	return &OutputWriter_GetOutputPath{Call: c}
}

// GetOutputPath provides a mock function with given fields:
func (_m *OutputWriter) GetOutputPath() storage.DataReference {
	ret := _m.Called()

	var r0 storage.DataReference
	if rf, ok := ret.Get(0).(func() storage.DataReference); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(storage.DataReference)
	}

	return r0
}

type OutputWriter_GetOutputPrefixPath struct {
	*mock.Call
}

func (_m OutputWriter_GetOutputPrefixPath) Return(_a0 storage.DataReference) *OutputWriter_GetOutputPrefixPath {
	return &OutputWriter_GetOutputPrefixPath{Call: _m.Call.Return(_a0)}
}

func (_m *OutputWriter) OnGetOutputPrefixPath() *OutputWriter_GetOutputPrefixPath {
	c := _m.On("GetOutputPrefixPath")
	return &OutputWriter_GetOutputPrefixPath{Call: c}
}

func (_m *OutputWriter) OnGetOutputPrefixPathMatch(matchers ...interface{}) *OutputWriter_GetOutputPrefixPath {
	c := _m.On("GetOutputPrefixPath", matchers...)
	return &OutputWriter_GetOutputPrefixPath{Call: c}
}

// GetOutputPrefixPath provides a mock function with given fields:
func (_m *OutputWriter) GetOutputPrefixPath() storage.DataReference {
	ret := _m.Called()

	var r0 storage.DataReference
	if rf, ok := ret.Get(0).(func() storage.DataReference); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(storage.DataReference)
	}

	return r0
}

type OutputWriter_Put struct {
	*mock.Call
}

func (_m OutputWriter_Put) Return(_a0 error) *OutputWriter_Put {
	return &OutputWriter_Put{Call: _m.Call.Return(_a0)}
}

func (_m *OutputWriter) OnPut(ctx context.Context, reader io.OutputReader) *OutputWriter_Put {
	c := _m.On("Put", ctx, reader)
	return &OutputWriter_Put{Call: c}
}

func (_m *OutputWriter) OnPutMatch(matchers ...interface{}) *OutputWriter_Put {
	c := _m.On("Put", matchers...)
	return &OutputWriter_Put{Call: c}
}

// Put provides a mock function with given fields: ctx, reader
func (_m *OutputWriter) Put(ctx context.Context, reader io.OutputReader) error {
	ret := _m.Called(ctx, reader)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, io.OutputReader) error); ok {
		r0 = rf(ctx, reader)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}