// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package validate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

func TestMeasureShardingKeyNil(t *testing.T) {
	measure := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  "test_measure",
			Group: "test_group",
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"service_id"},
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			},
		},
	}
	err := Measure(measure)
	assert.NoError(t, err)
}

func TestMeasureShardingKeyValidation(t *testing.T) {
	tests := []struct {
		name        string
		entity      []string
		shardingKey []string
		wantErr     bool
	}{
		{
			name:        "valid sharding key (identical to entity)",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"service_id", "instance_id"},
			wantErr:     false,
		},
		{
			name:        "valid sharding key (prefix of entity, not identical)",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"service_id"},
			wantErr:     false,
		},
		{
			name:        "invalid sharding key (subset with different tag)",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"instance_id"},
			wantErr:     true,
		},
		{
			name:        "invalid sharding key (same tags, wrong order)",
			entity:      []string{"service_id", "instance_id"},
			shardingKey: []string{"instance_id", "service_id"},
			wantErr:     true,
		},
		{
			name:        "invalid sharding key (superset of entity)",
			entity:      []string{"service_id"},
			shardingKey: []string{"service_id", "instance_id"},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			measure := &databasev1.Measure{
				Metadata: &commonv1.Metadata{Name: "test", Group: "group"},
				Entity:   &databasev1.Entity{TagNames: tt.entity},
				TagFamilies: []*databasev1.TagFamilySpec{
					{
						Name: "f1",
						Tags: []*databasev1.TagSpec{
							{Name: "service_id", Type: databasev1.TagType_TAG_TYPE_STRING},
							{Name: "instance_id", Type: databasev1.TagType_TAG_TYPE_STRING},
						},
					},
				},
			}
			if tt.shardingKey != nil {
				measure.ShardingKey = &databasev1.ShardingKey{TagNames: tt.shardingKey}
			}

			err := Measure(measure)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "must be a prefix of Entity tags")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
