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
