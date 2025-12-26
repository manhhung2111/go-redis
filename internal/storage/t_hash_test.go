package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHGet_GetExistingField(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	value, exists := s.HGet("key1", "field1")
	assert.True(t, exists)
	assert.Equal(t, "value1", value)
}

func TestHGet_GetNonExistentField(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	value, exists := s.HGet("key1", "field2")
	assert.False(t, exists)
	assert.Equal(t, "", value)
}

func TestHGet_GetFromNonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	value, exists := s.HGet("key1", "field1")
	assert.False(t, exists)
	assert.Equal(t, "", value)
}

func TestHGet_GetFromWrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string value"}

	value, exists := s.HGet("key1", "field1")
	assert.False(t, exists)
	assert.Equal(t, "", value)
}

func TestHGetAll_GetAllFields(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1", "field2": "value2"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HGetAll("key1")
	assert.Len(t, result, 4)
	assert.Contains(t, result, "field1")
	assert.Contains(t, result, "value1")
	assert.Contains(t, result, "field2")
	assert.Contains(t, result, "value2")
}

func TestHGetAll_EmptyHash(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HGetAll("key1")
	assert.Empty(t, result)
}

func TestHGetAll_NonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.HGetAll("key1")
	assert.Empty(t, result)
}

func TestHGetAll_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string value"}

	result := s.HGetAll("key1")
	assert.Empty(t, result)
}

func TestHMGet_GetMultipleExistingFields(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HMGet("key1", []string{"field1", "field3"})
	require.Len(t, result, 2)
	assert.Equal(t, "value1", *result[0])
	assert.Equal(t, "value3", *result[1])
}

func TestHMGet_MixExistingAndMissingFields(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HMGet("key1", []string{"field1", "field2", "field3"})
	require.Len(t, result, 3)
	assert.NotNil(t, result[0])
	assert.Nil(t, result[1])
	assert.Nil(t, result[2])
}

func TestHMGet_NonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.HMGet("key1", []string{"field1", "field2"})
	require.Len(t, result, 2)
	assert.Nil(t, result[0])
	assert.Nil(t, result[1])
}

func TestHMGet_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string value"}

	result := s.HMGet("key1", []string{"field1", "field2"})
	require.Len(t, result, 2)
	assert.Nil(t, result[0])
	assert.Nil(t, result[1])
}

func TestHMGet_EmptyFieldsSlice(t *testing.T) {
	s := NewStore().(*store)

	result := s.HMGet("key1", []string{})
	assert.Empty(t, result)
}

func TestHIncrBy_ExistingField(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"counter": "10"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result)
}

func TestHIncrBy_NewFieldExistingHash(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result)
}

func TestHIncrBy_NewHashCreated(t *testing.T) {
	s := NewStore().(*store)

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result)

	rObj, exists := s.data["key1"]
	assert.True(t, exists)
	assert.Equal(t, ObjHash, rObj.Type)
}

func TestHIncrBy_NegativeIncrement(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"counter": "10"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result, err := s.HIncrBy("key1", "counter", -3)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), result)
}

func TestHIncrBy_NonIntegerValue(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"counter": "not a number"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.Error(t, err)
	assert.Equal(t, int64(0), result)
}

func TestHIncrBy_PanicWrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string value"}

	assert.Panics(t, func() {
		s.HIncrBy("key1", "counter", 5)
	})
}

func TestHKeys_Normal(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1", "field2": "value2"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HKeys("key1")
	assert.Len(t, result, 2)
	assert.Contains(t, result, "field1")
	assert.Contains(t, result, "field2")
}

func TestHKeys_EmptyHash(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HKeys("key1")
	assert.Empty(t, result)
}

func TestHKeys_NonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.HKeys("key1")
	assert.Empty(t, result)
}

func TestHKeys_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string value"}

	result := s.HKeys("key1")
	assert.Empty(t, result)
}

func TestHVals_Normal(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1", "field2": "value2"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HVals("key1")
	assert.Len(t, result, 2)
	assert.Contains(t, result, "value1")
	assert.Contains(t, result, "value2")
}

func TestHVals_EmptyHash(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HVals("key1")
	assert.Empty(t, result)
}

func TestHVals_NonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.HVals("key1")
	assert.Empty(t, result)
}

func TestHVals_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string value"}

	result := s.HVals("key1")
	assert.Empty(t, result)
}

func TestHLen_Normal(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HLen("key1")
	assert.Equal(t, uint32(3), result)
}

func TestHLen_EmptyHash(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HLen("key1")
	assert.Equal(t, uint32(0), result)
}

func TestHLen_NonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.HLen("key1")
	assert.Equal(t, uint32(0), result)
}

func TestHLen_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string value"}

	result := s.HLen("key1")
	assert.Equal(t, uint32(0), result)
}

func TestHSet_NewHash(t *testing.T) {
	s := NewStore().(*store)

	added := s.HSet("key1", map[string]string{"f1": "v1", "f2": "v2"})
	assert.Equal(t, int64(2), added)
}

func TestHSet_ExistingHash(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	added := s.HSet("key1", map[string]string{"f2": "v2", "f3": "v3"})
	assert.Equal(t, int64(2), added)
}

func TestHSet_UpdateExistingField(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	added := s.HSet("key1", map[string]string{"f1": "nv1", "f3": "v3"})
	assert.Equal(t, int64(1), added)
}

func TestHSet_EmptyMap(t *testing.T) {
	s := NewStore().(*store)

	added := s.HSet("key1", map[string]string{})
	assert.Equal(t, int64(0), added)
}

func TestHSet_PanicWrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"}

	assert.Panics(t, func() {
		s.HSet("key1", map[string]string{"f": "v"})
	})
}

func TestHSetNx_NewHash(t *testing.T) {
	s := NewStore().(*store)

	result := s.HSetNx("key1", "field1", "value1")
	assert.Equal(t, int64(1), result)
}

func TestHSetNx_NewField(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HSetNx("key1", "field2", "value2")
	assert.Equal(t, int64(1), result)
}

func TestHSetNx_ExistingField(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HSetNx("key1", "field1", "new")
	assert.Equal(t, int64(0), result)
}

func TestHSetNx_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"}

	result := s.HSetNx("key1", "field1", "value1")
	assert.Equal(t, int64(0), result)
}

func TestHDel_DeleteExistingFields(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	deleted := s.HDel("key1", []string{"f1", "f3"})
	assert.Equal(t, int64(2), deleted)
}

func TestHDel_DeleteAllFieldsRemovesKey(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	deleted := s.HDel("key1", []string{"f1", "f2"})
	assert.Equal(t, int64(2), deleted)
	_, exists := s.data["key1"]
	assert.False(t, exists)
}

func TestHDel_NonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	deleted := s.HDel("key1", []string{"f"})
	assert.Equal(t, int64(0), deleted)
}

func TestHDel_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"}

	deleted := s.HDel("key1", []string{"f"})
	assert.Equal(t, int64(0), deleted)
}

func TestHExists_FieldExists(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HExists("key1", "field1")
	assert.Equal(t, int64(1), result)
}

func TestHExists_FieldMissing(t *testing.T) {
	s := NewStore().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data["key1"] = &RObj{Type: ObjHash, Encoding: EncHashTable, Value: hash}

	result := s.HExists("key1", "field2")
	assert.Equal(t, int64(0), result)
}

func TestHExists_NonExistentKey(t *testing.T) {
	s := NewStore().(*store)

	result := s.HExists("key1", "field1")
	assert.Equal(t, int64(0), result)
}

func TestHExists_WrongType(t *testing.T) {
	s := NewStore().(*store)
	s.data["key1"] = &RObj{Type: ObjString, Encoding: EncRaw, Value: "string"}

	result := s.HExists("key1", "field1")
	assert.Equal(t, int64(0), result)
}