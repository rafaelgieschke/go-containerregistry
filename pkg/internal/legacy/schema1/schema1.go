// Copyright 2020 Google LLC All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema1

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

type Fslayer struct {
	BlobSum string `json:"blobSum"`
}

type Manifest struct {
	FSLayers []Fslayer `json:"fsLayers"`
}

type WithBlob interface {
	Blob(h v1.Hash) (io.ReadCloser, error)
}

type WithLayerByDigest interface {
	LayerByDigest(h v1.Hash) (v1.Layer, error)
}

type schema1Layer struct {
	source WithBlob
	digest v1.Hash
}

// These are all the methods that
func (l *schema1Layer) Compressed() (io.ReadCloser, error) {
	return l.source.Blob(l.digest)
}

func (l *schema1Layer) Digest() (v1.Hash, error) {
	return l.digest, nil
}

func (l *schema1Layer) MediaType() (types.MediaType, error) {
	return types.DockerLayer, nil
}

// We don't actually know this, hopefully it's okay.
func (l *schema1Layer) Size() (int64, error) {
	return 0, fmt.Errorf("schema 1 layer %q can't know size", l.digest)
}

type schema1Image struct {
	manifest  []byte
	source    WithBlob
	digest    v1.Hash
	mediaType types.MediaType

	// Embed this to "implement" stuff that's impossible for now with a panic.
	v1.Image
}

func (i *schema1Image) Layers() ([]v1.Layer, error) {
	m := Manifest{}
	if err := json.NewDecoder(bytes.NewReader(i.manifest)).Decode(&m); err != nil {
		return nil, err
	}

	layers := []v1.Layer{}
	for _, fs := range m.FSLayers {
		h, err := v1.NewHash(fs.BlobSum)
		if err != nil {
			return nil, err
		}

		layer, err := i.LayerByDigest(h)
		if err != nil {
			return nil, err
		}

		layers = append(layers, layer)
	}

	return layers, nil
}

func (i *schema1Image) LayerByDigest(h v1.Hash) (v1.Layer, error) {
	if wl, ok := i.source.(WithLayerByDigest); ok {
		return wl.LayerByDigest(h)
	}

	compressed := &schema1Layer{
		source: i.source,
		digest: i.digest,
	}
	return partial.CompressedToLayer(compressed)
}

func (i *schema1Image) RawManifest() ([]byte, error) {
	return i.manifest, nil
}

func (i *schema1Image) RawConfigFile() ([]byte, error) {
	r, err := empty.Layer.Compressed()
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(r)
}

func (i *schema1Image) ConfigName() (v1.Hash, error) {
	return empty.Layer.Digest()
}

func (i *schema1Image) Size() (int64, error) {
	return int64(len(i.manifest)), nil
}

func (i *schema1Image) MediaType() (types.MediaType, error) {
	return i.mediaType, nil
}

// Child is a hack to make copying an index with a schema 1 child
// work if the index supports Blob. This is exceedinly rare, but valid.
//
// We should probably just give in and expose a v1.Image implementation of schema 1.
//
// If the source also implements WithLayer, we'll use that instead of Blob.
//
// TODO(#819): Everything should support Blob.
func Child(source WithBlob, h v1.Hash, mt types.MediaType) (v1.Image, error) {
	manifestBlob, err := source.Blob(h)
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(manifestBlob)
	if err != nil {
		return nil, err
	}

	return &schema1Image{
		manifest:  b,
		source:    source,
		digest:    h,
		mediaType: mt,
	}, nil
}

func New(source WithBlob, h v1.Hash, mt types.MediaType, b []byte) v1.Image {
	return &schema1Image{
		manifest:  b,
		source:    source,
		digest:    h,
		mediaType: mt,
	}
}
