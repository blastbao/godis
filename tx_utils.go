package godis

import (
	"github.com/hdt3213/godis/aof"
	"github.com/hdt3213/godis/lib/utils"
	"strconv"
)

func readFirstKey(args [][]byte) ([]string, []string) {
	// assert len(args) > 0
	key := string(args[0])
	return nil, []string{key}
}

// 取 args[0]
func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func writeAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return keys, nil
}

func readAllKeys(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

func noPrepare(args [][]byte) ([]string, []string) {
	return nil, nil
}

func rollbackFirstKey(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	return rollbackGivenKeys(db, key)
}

func rollbackGivenKeys(db *DB, keys ...string) []CmdLine {
	var undoCmdLines [][][]byte
	// 遍历所有 keys
	for _, key := range keys {
		// 取 key 的当前值
		entity, ok := db.GetEntity(key)
		// 若 key 不存在，它的回滚操作即: "DEL key"
		if !ok {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("DEL", key),
			)
		// 若 key 已存在，它的回滚操作即:
		//   a. DEL key
		//   b. SET key origin_value
		//   c. PEXPIREAT key timestamp
		// 也即: 先删、恢复旧值、设置超时
		// (为啥要先删除呢？直接覆盖不行吗？)
		} else {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("DEL", key), // clean existed first
				aof.EntityToCmd(key, entity).Args,
				toTTLCmd(db, key).Args,
			)
		}
	}
	return undoCmdLines
}

func rollbackHashFields(db *DB, key string, fields ...string) []CmdLine {
	var undoCmdLines [][][]byte
	dict, errReply := db.getAsDict(key)
	if errReply != nil {
		return nil
	}
	if dict == nil {
		undoCmdLines = append(undoCmdLines,
			utils.ToCmdLine("DEL", key),
		)
		return undoCmdLines
	}
	for _, field := range fields {
		entity, ok := dict.Get(field)
		if !ok {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("HDEL", key, field),
			)
		} else {
			value, _ := entity.([]byte)
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("HSET", key, field, string(value)),
			)
		}
	}
	return undoCmdLines
}

func prepareSetCalculate(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return nil, keys
}

func prepareSetCalculateStore(args [][]byte) ([]string, []string) {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}
	return []string{dest}, keys
}

func rollbackSetMembers(db *DB, key string, members ...string) []CmdLine {
	var undoCmdLines [][][]byte
	set, errReply := db.getAsSet(key)
	if errReply != nil {
		return nil
	}
	if set == nil {
		undoCmdLines = append(undoCmdLines,
			utils.ToCmdLine("DEL", key),
		)
		return undoCmdLines
	}
	for _, member := range members {
		ok := set.Has(member)
		if !ok {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("SREM", key, member),
			)
		} else {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("SADD", key, member),
			)
		}
	}
	return undoCmdLines
}

// undoSetChange rollbacks SADD and SREM command
func undoSetChange(db *DB, args [][]byte) []CmdLine {
	key := string(args[0])
	memberArgs := args[1:]
	members := make([]string, len(memberArgs))
	for i, mem := range memberArgs {
		members[i] = string(mem)
	}
	return rollbackSetMembers(db, key, members...)
}

func rollbackZSetFields(db *DB, key string, fields ...string) []CmdLine {
	var undoCmdLines [][][]byte

	// 获取 key 关联的 zset
	zset, errReply := db.getAsSortedSet(key)
	if errReply != nil {
		return nil
	}

	// 如果 key 不存在，则它的回滚命令为 "DEL key"
	if zset == nil {
		undoCmdLines = append(undoCmdLines,
			utils.ToCmdLine("DEL", key),
		)
		return undoCmdLines
	}

	// 如果 key 存在，则检查 fields 是否存在
	for _, field := range fields {
		elem, ok := zset.Get(field)

		// 如果 field 不存在，其回滚操作为 "ZREM key field"
		if !ok {
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("ZREM", key, field),
			)
		// 如果 field 已存在，其回滚操作为 "ZADD key origin_score origin_field"
		} else {
			score := strconv.FormatFloat(elem.Score, 'f', -1, 64)
			undoCmdLines = append(undoCmdLines,
				utils.ToCmdLine("ZADD", key, score, field),
			)
		}
	}
	return undoCmdLines
}
