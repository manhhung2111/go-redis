package storage

import (
	"testing"

	"github.com/manhhung2111/go-redis/internal/config"
	"github.com/manhhung2111/go-redis/internal/storage/data_structure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStoreHash() Store {
	return NewStore(config.NewConfig())
}

func TestHGet_GetExistingField(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	value, err := s.HGet("key1", "field1")
	assert.NoError(t, err)
	assert.Equal(t, "value1", *value)
}

func TestHGet_GetNonExistentField(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	value, err := s.HGet("key1", "field2")
	assert.NoError(t, err)
	assert.Nil(t, value)
}

func TestHGet_GetFromNonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	value, err := s.HGet("key1", "field1")
	assert.NoError(t, err)
	assert.Nil(t, value)
}

func TestHGet_GetFromWrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string value"})

	value, err := s.HGet("key1", "field1")
	assert.Error(t, err)
	assert.Nil(t, value)
}

func TestHGetAll_GetAllFields(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1", "field2": "value2"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HGetAll("key1")
	assert.Len(t, result, 4)
	assert.Contains(t, result, "field1")
	assert.Contains(t, result, "value1")
	assert.Contains(t, result, "field2")
	assert.Contains(t, result, "value2")
}

func TestHGetAll_EmptyHash(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HGetAll("key1")
	assert.Empty(t, result)
}

func TestHGetAll_NonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HGetAll("key1")
	assert.Empty(t, result)
}

func TestHGetAll_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string value"})

	result, _ := s.HGetAll("key1")
	assert.Empty(t, result)
}

func TestHMGet_GetMultipleExistingFields(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HMGet("key1", []string{"field1", "field3"})
	require.Len(t, result, 2)
	assert.Equal(t, "value1", *result[0])
	assert.Equal(t, "value3", *result[1])
}

func TestHMGet_MixExistingAndMissingFields(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HMGet("key1", []string{"field1", "field2", "field3"})
	require.Len(t, result, 3)
	assert.NotNil(t, result[0])
	assert.Nil(t, result[1])
	assert.Nil(t, result[2])
}

func TestHMGet_NonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HMGet("key1", []string{"field1", "field2"})
	require.Len(t, result, 2)
	assert.Nil(t, result[0])
	assert.Nil(t, result[1])
}

func TestHMGet_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string value"})

	_, err := s.HMGet("key1", []string{"field1", "field2"})
	assert.Error(t, err)
}

func TestHMGet_EmptyFieldsSlice(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HMGet("key1", []string{})
	assert.Empty(t, result)
}

func TestHIncrBy_ExistingField(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"counter": "10"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(15), result)
}

func TestHIncrBy_NewFieldExistingHash(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result)
}

func TestHIncrBy_NewHashCreated(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.NoError(t, err)
	assert.Equal(t, int64(5), result)

	rObj, exists := s.data.Get("key1")
	assert.True(t, exists)
	assert.Equal(t, ObjHash, rObj.objType)
}

func TestHIncrBy_NegativeIncrement(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"counter": "10"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, err := s.HIncrBy("key1", "counter", -3)
	assert.NoError(t, err)
	assert.Equal(t, int64(7), result)
}

func TestHIncrBy_NonIntegerValue(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"counter": "not a number"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.Error(t, err)
	assert.Equal(t, int64(0), result)
}

func TestHIncrBy_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string value"})

	result, err := s.HIncrBy("key1", "counter", 5)
	assert.Error(t, err)
	assert.Equal(t, int64(0), result)
}

func TestHKeys_Normal(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1", "field2": "value2"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HKeys("key1")
	assert.Len(t, result, 2)
	assert.Contains(t, result, "field1")
	assert.Contains(t, result, "field2")
}

func TestHKeys_EmptyHash(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HKeys("key1")
	assert.Empty(t, result)
}

func TestHKeys_NonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HKeys("key1")
	assert.Empty(t, result)
}

func TestHKeys_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string value"})

	result, _ := s.HKeys("key1")
	assert.Empty(t, result)
}

func TestHVals_Normal(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1", "field2": "value2"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HVals("key1")
	assert.Len(t, result, 2)
	assert.Contains(t, result, "value1")
	assert.Contains(t, result, "value2")
}

func TestHVals_EmptyHash(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HVals("key1")
	assert.Empty(t, result)
}

func TestHVals_NonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HVals("key1")
	assert.Empty(t, result)
}

func TestHVals_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string value"})

	result, _ := s.HVals("key1")
	assert.Empty(t, result)
}

func TestHLen_Normal(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HLen("key1")
	assert.Equal(t, uint32(3), result)
}

func TestHLen_EmptyHash(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HLen("key1")
	assert.Equal(t, uint32(0), result)
}

func TestHLen_NonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HLen("key1")
	assert.Equal(t, uint32(0), result)
}

func TestHLen_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string value"})

	result, _ := s.HLen("key1")
	assert.Equal(t, uint32(0), result)
}

func TestHSet_NewHash(t *testing.T) {
	s := newTestStoreHash().(*store)

	added, _ := s.HSet("key1", map[string]string{"f1": "v1", "f2": "v2"})
	assert.Equal(t, int64(2), added)
}

func TestHSet_ExistingHash(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	added, _ := s.HSet("key1", map[string]string{"f2": "v2", "f3": "v3"})
	assert.Equal(t, int64(2), added)
}

func TestHSet_UpdateExistingField(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	added, _ := s.HSet("key1", map[string]string{"f1": "nv1", "f3": "v3"})
	assert.Equal(t, int64(1), added)
}

func TestHSet_EmptyMap(t *testing.T) {
	s := newTestStoreHash().(*store)

	added, _ := s.HSet("key1", map[string]string{})
	assert.Equal(t, int64(0), added)
}

func TestHSet_PanicWrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string"})

	_, err := s.HSet("key1", map[string]string{"f": "v"})
	assert.Error(t, err)
}

func TestHSetNx_NewHash(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HSetNx("key1", "field1", "value1")
	assert.Equal(t, int64(1), result)
}

func TestHSetNx_NewField(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HSetNx("key1", "field2", "value2")
	assert.Equal(t, int64(1), result)
}

func TestHSetNx_ExistingField(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HSetNx("key1", "field1", "new")
	assert.Equal(t, int64(0), result)
}

func TestHSetNx_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string"})

	result, _ := s.HSetNx("key1", "field1", "value1")
	assert.Equal(t, int64(0), result)
}

func TestHDel_DeleteExistingFields(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	deleted, _ := s.HDel("key1", []string{"f1", "f3"})
	assert.Equal(t, int64(2), deleted)
}

func TestHDel_DeleteAllFieldsRemovesKey(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"f1": "v1", "f2": "v2"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	deleted, _ := s.HDel("key1", []string{"f1", "f2"})
	assert.Equal(t, int64(2), deleted)
	_, exists := s.data.Get("key1")
	assert.False(t, exists)
}

func TestHDel_NonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	deleted, _ := s.HDel("key1", []string{"f"})
	assert.Equal(t, int64(0), deleted)
}

func TestHDel_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string"})

	deleted, _ := s.HDel("key1", []string{"f"})
	assert.Equal(t, int64(0), deleted)
}

func TestHExists_FieldExists(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HExists("key1", "field1")
	assert.Equal(t, int64(1), result)
}

func TestHExists_FieldMissing(t *testing.T) {
	s := newTestStoreHash().(*store)
	hash := data_structure.NewHash()
	hash.Set(map[string]string{"field1": "value1"})
	s.data.Set("key1", &RObj{objType: ObjHash, encoding: EncHashTable, value: hash})

	result, _ := s.HExists("key1", "field2")
	assert.Equal(t, int64(0), result)
}

func TestHExists_NonExistentKey(t *testing.T) {
	s := newTestStoreHash().(*store)

	result, _ := s.HExists("key1", "field1")
	assert.Equal(t, int64(0), result)
}

func TestHExists_WrongType(t *testing.T) {
	s := newTestStoreHash().(*store)
	s.data.Set("key1", &RObj{objType: ObjString, encoding: EncRaw, value: "string"})

	result, _ := s.HExists("key1", "field1")
	assert.Equal(t, int64(0), result)
}
