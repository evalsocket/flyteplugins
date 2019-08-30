package flytek8s

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/lyft/flyteidl/gen/pb-go/flyteidl/core"
	"github.com/lyft/flytestdlib/storage"

	"github.com/lyft/flyteplugins/go/tasks/v1/pluginmachinery/io"
)

type remoteFileOutputReader struct {
	outPath        io.OutputFilePaths
	store          storage.ComposedProtobufStore
	maxPayloadSize int64
}

func (r remoteFileOutputReader) IsError(ctx context.Context) (bool, error) {
	metadata, err := r.store.Head(ctx, r.outPath.GetErrorPath())
	if err != nil {
		return false, errors.Wrapf(err, "failed to read error file @[%s]", r.outPath.GetErrorPath())
	}
	if metadata.Exists() {
		if metadata.Size() > r.maxPayloadSize {
			return false, errors.Wrapf(err, "error file @[%s] is too large [%d] bytes, max allowed [%d] bytes", r.outPath.GetErrorPath(), metadata.Size(), r.maxPayloadSize)
		}
		return true, nil
	}
	return false, nil
}

func (r remoteFileOutputReader) ReadError(ctx context.Context) (io.ExecutionError, error) {
	errorDoc := &core.ErrorDocument{}
	err := r.store.ReadProtobuf(ctx, r.outPath.GetErrorPath(), errorDoc)
	if err != nil {
		if storage.IsNotFound(err) {
			return io.ExecutionError{
				IsRecoverable: true,
				ExecutionError: &core.ExecutionError{
					Code:    "ErrorFileNotFound",
					Message: err.Error(),
				},
			}, nil
		}
		return io.ExecutionError{}, errors.Wrapf(err, "failed to read error data from task @[%s]", r.outPath.GetErrorPath())
	}

	if errorDoc.Error == nil {
		return io.ExecutionError{
			IsRecoverable: true,
			ExecutionError: &core.ExecutionError{
				Code:    "ErrorFileBadFormat",
				Message: fmt.Sprintf("error not formatted correctly, nil error @path [%s]", r.outPath.GetErrorPath()),
			},
		}, nil
	}

	ee := io.ExecutionError{
		ExecutionError: &core.ExecutionError{
			Code:    errorDoc.Error.Code,
			Message: errorDoc.Error.Message,
		},
	}
	if errorDoc.Error.Kind == core.ContainerError_RECOVERABLE {
		ee.IsRecoverable = true
	}
	return ee, nil
}

func (r remoteFileOutputReader) Exists(ctx context.Context) (bool, error) {
	md, err := r.store.Head(ctx, r.outPath.GetOutputPath())
	if err != nil {
		return false, err
	}
	if md.Exists() {
		if md.Size() > r.maxPayloadSize {
			return false, errors.Wrapf(err, "error file @[%s] is too large [%d] bytes, max allowed [%d] bytes", r.outPath.GetErrorPath(), md.Size(), r.maxPayloadSize)
		}
		return true, nil
	}
	return false, nil
}

func (r remoteFileOutputReader) Read(ctx context.Context) (*core.LiteralMap, *io.ExecutionError, error) {

	d := &core.LiteralMap{}
	if err := r.store.ReadProtobuf(ctx, r.outPath.GetOutputPath(), d); err != nil {
		// TODO change flytestdlib to return protobuf unmarshal errors separately. As this can indicate malformed output and we should catch that
		return nil, nil, fmt.Errorf("failed to read data from dataDir [%v]. Error: %v", r.outPath.GetOutputPath(), err)
	}

	if d.Literals == nil {
		return nil, &io.ExecutionError{
			IsRecoverable: true,
			ExecutionError: &core.ExecutionError{
				Code:    "No outputs produced",
				Message: fmt.Sprintf("outputs not found at [%s]", r.outPath.GetOutputPath()),
			},
		}, nil
	}

	return d, nil, nil
}

func (r remoteFileOutputReader) IsFile(ctx context.Context) bool {
	return true
}

func NewRemoteFileOutputReader(_ context.Context, store storage.ComposedProtobufStore, outPaths io.OutputFilePaths, maxDatasetSize int64) remoteFileOutputReader {
	return remoteFileOutputReader{
		outPath:        outPaths,
		store:          store,
		maxPayloadSize: maxDatasetSize,
	}
}