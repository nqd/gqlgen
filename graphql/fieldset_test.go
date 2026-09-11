package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vektah/gqlparser/v2/ast"
)

func TestFieldSet_MarshalGQL(t *testing.T) {
	t.Run("Should_Deduplicate_Keys", func(t *testing.T) {
		fs := NewFieldSet([]CollectedField{
			{Field: &ast.Field{Alias: "__typename"}},
			{Field: &ast.Field{Alias: "__typename"}},
		})
		fs.Values[0] = MarshalString("A")
		fs.Values[1] = MarshalString("A")

		b := bytes.NewBuffer(nil)
		fs.MarshalGQL(b)

		assert.JSONEq(t, "{\"__typename\":\"A\"}", b.String())
	})

	// Exercises the map-upgrade path: a selection larger than the stack buffer
	// (16 aliases) with a duplicate beyond the buffer boundary.
	t.Run("Should_Deduplicate_Keys_Beyond_Stack_Buffer", func(t *testing.T) {
		const n = 40
		fields := make([]CollectedField, 0, n+1)
		for i := range n {
			fields = append(fields, CollectedField{Field: &ast.Field{Alias: fmt.Sprintf("f%d", i)}})
		}
		// Duplicate of the very first alias, appearing after the buffer upgrade.
		fields = append(fields, CollectedField{Field: &ast.Field{Alias: "f0"}})

		fs := NewFieldSet(fields)
		for i := range fs.Values {
			fs.Values[i] = MarshalInt(i)
		}

		b := bytes.NewBuffer(nil)
		fs.MarshalGQL(b)

		expected := make(map[string]int, n)
		for i := range n {
			expected[fmt.Sprintf("f%d", i)] = i // "f0" keeps its first value, 0
		}
		expectedJSON, err := json.Marshal(expected)
		assert.NoError(t, err)
		assert.JSONEq(t, string(expectedJSON), b.String())
	})
}

func BenchmarkMarshalFieldSet(b *testing.B) {
	fields := make([]CollectedField, 0, 8)
	for i := range 8 {
		fields = append(fields, CollectedField{Field: &ast.Field{Alias: fmt.Sprintf("field%d", i)}})
	}
	values := make([]Marshaler, len(fields))
	for i := range values {
		values[i] = MarshalInt(i)
	}

	b.ReportAllocs()
	for b.Loop() {
		var buf bytes.Buffer
		marshalFieldSet(&buf, fields, nil, values)
	}
}

func addConcurrentFieldAndReturnIndex(
	t *testing.T,
	fieldSet *FieldSet,
	field *ast.Field,
	resolver func(context.Context) Marshaler,
) int {
	t.Helper()
	fieldSet.AddField(CollectedField{Field: field})
	i := len(fieldSet.Values) - 1
	fieldSet.Concurrently(i, resolver)
	return i
}

func TestFieldSetView(t *testing.T) {
	t.Parallel()
	t.Run("properly_yields_values_and_takes_them", func(t *testing.T) {
		synctest.Test(
			t,
			func(t *testing.T) {
				// Arrange
				fieldSet := NewFieldSet(nil)
				view1 := fieldSet.NewView()
				view2 := fieldSet.NewView()
				view3 := fieldSet.NewView()

				slowestField := addConcurrentFieldAndReturnIndex(
					t,
					fieldSet,
					&ast.Field{
						Alias: "slowestField",
					},
					func(ctx context.Context) Marshaler {
						time.Sleep(time.Second * 3)
						return MarshalString("slowestFieldValue")
					},
				)

				secondSlowestField := addConcurrentFieldAndReturnIndex(
					t,
					fieldSet,
					&ast.Field{Alias: "secondSlowestField"},
					func(ctx context.Context) Marshaler {
						time.Sleep(time.Second * 2)
						return MarshalString("secondSlowestFieldValue")
					},
				)

				fastestField := addConcurrentFieldAndReturnIndex(
					t,
					fieldSet,
					&ast.Field{Alias: "fastestField"},
					func(ctx context.Context) Marshaler {
						time.Sleep(time.Second * 1)
						return MarshalString("fastestFieldValue")
					},
				)

				view1.AddIndices(slowestField, fastestField)
				view2.AddIndices(fastestField)
				view3.AddIndices(fastestField, secondSlowestField)

				resultCh := make(chan *FieldSetView)
				view1.SetOnComplete(func(ctx context.Context) {
					resultCh <- view1
				})
				view2.SetOnComplete(func(ctx context.Context) {
					resultCh <- view2
				})
				view3.SetOnComplete(func(ctx context.Context) {
					resultCh <- view3
				})

				// Act
				go fieldSet.Dispatch(t.Context())

				// Assert
				expectedResults := []string{
					`{"fastestField":"fastestFieldValue"}`,
					`{"secondSlowestField":"secondSlowestFieldValue"}`,
					`{"slowestField":"slowestFieldValue"}`,
				}
				for _, expected := range expectedResults {
					synctest.Wait()
					view := <-resultCh
					var buf bytes.Buffer
					view.MarshalGQL(&buf)
					assert.JSONEq(t, expected, buf.String())
				}
			},
		)
	})
}
