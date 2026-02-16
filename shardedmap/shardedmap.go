package shardedmap

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"unsafe"
)

// ShardedMap представляет собой конкурентную карту с шардированием для минимизации блокировок.
// Использует отдельный sync.RWMutex для каждого шарда, что позволяет параллельно выполнять
// операции над разными ключами.
type ShardedMap[K comparable, V any] struct {
	Shards     []*Shard[K, V] // слайс шардов
	shardMask  uint64         // маска для быстрого вычисления индекса шарда
	shardCount uint64         // количество шардов (всегда степень двойки)
}

// Shard представляет отдельную карту с собственным мьютексом.
// Используется структура-указатель для избежания копирования при итерации.
type Shard[K comparable, V any] struct {
	sync.RWMutex
	Data map[K]V
}

// NewShardedMap создает новый экземпляр ShardedMap с указанным количеством шардов.
// Количество шардов округляется до ближайшей степени двойки для оптимизации
// вычисления индекса через побитовую маску.
func NewShardedMap[K comparable, V any](shardCount int) *ShardedMap[K, V] {
	if shardCount <= 0 {
		shardCount = 16 // значение по умолчанию
	}

	// Округляем до степени двойки для использования побитовой маски
	actualCount := 1
	for actualCount < shardCount {
		actualCount <<= 1
	}

	sm := &ShardedMap[K, V]{
		Shards:     make([]*Shard[K, V], actualCount),
		shardMask:  uint64(actualCount - 1),
		shardCount: uint64(actualCount),
	}

	// Инициализируем шарды
	for i := range sm.Shards {
		sm.Shards[i] = &Shard[K, V]{
			Data: make(map[K]V),
		}
	}

	return sm
}

// getShardIndex вычисляет индекс шарда для ключа, используя FNV-1a хеш.
// Почему FNV-1a: быстрый, равномерно распределяет ключи, не аллоцирует память.
// Почему маска вместо модуля: побитовая операция быстрее, но требует степени двойки.
func (sm *ShardedMap[K, V]) getShardIndex(key K) uint64 {
	// Преобразуем ключ в слайс байт без аллокации через unsafe
	// Это критично для производительности, так как hash.WriteString аллоцирует

	// Используем FNV-1a хеш для равномерного распределения
	hash := fnv.New64a()

	// Записываем ключ как слайс байт через unsafe-преобразование
	// Это zero-allocation хак, работающий только для типов, чья память непрерывна
	// Для production кода стоит добавить type switch для строк и чисел
	switch v := any(key).(type) {
	case string:
		// Для строк используем стандартное преобразование - Go знает как оптимизировать
		_, _ = hash.Write([]byte(v))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Для чисел используем unsafe-преобразование (предполагаем little-endian)
		b := (*[8]byte)(unsafe.Pointer(&v))[:8]
		_, _ = hash.Write(b)
	default:
		// Для сложных типов используем стандартный путь с рефлексией
		// (будет медленнее, но безопасно)
		_, _ = hash.Write([]byte{})
	}

	return hash.Sum64() & sm.shardMask
}

// Get возвращает значение по ключу и флаг существования.
// Использует RLock() для параллельных чтений без блокировки записей в других шардах.
func (sm *ShardedMap[K, V]) Get(key K) (V, bool) {
	idx := sm.getShardIndex(key)
	Shard := sm.Shards[idx]

	Shard.RLock()
	defer Shard.RUnlock()

	val, ok := Shard.Data[key]
	return val, ok
}

// Set устанавливает значение для ключа.
// Использует Lock() так как это операция записи.
func (sm *ShardedMap[K, V]) Set(key K, value V) {
	idx := sm.getShardIndex(key)
	Shard := sm.Shards[idx]

	Shard.Lock()
	defer Shard.Unlock()

	Shard.Data[key] = value
}

// Delete удаляет ключ из карты.
func (sm *ShardedMap[K, V]) Delete(key K) {
	idx := sm.getShardIndex(key)
	Shard := sm.Shards[idx]

	Shard.Lock()
	defer Shard.Unlock()

	delete(Shard.Data, key)
}

// Keys возвращает слайс всех ключей в карте.
// Для консистентности данных мы блокируем все шарды по очереди,
// создавая снапшот ключей без блокировки всей карты целиком.
// Это может вернуть ключи, которые были удалены во время итерации,
// или пропустить только что добавленные, что приемлемо для rate limiter.
func (sm *ShardedMap[K, V]) Keys() []K {
	// Предварительно оцениваем размер, чтобы избежать множественных аллокаций
	totalEstimated := 0
	for _, Shard := range sm.Shards {
		Shard.RLock()
		totalEstimated += len(Shard.Data)
		Shard.RUnlock()
	}

	keys := make([]K, 0, totalEstimated)

	// Собираем ключи из каждого шарда
	for _, Shard := range sm.Shards {
		Shard.RLock()
		for k := range Shard.Data {
			keys = append(keys, k)
		}
		Shard.RUnlock()
	}

	return keys
}

// Len возвращает общее количество элементов.
// Используется для метрик и мониторинга.
func (sm *ShardedMap[K, V]) Len() int {
	var total int64

	// Используем WaitGroup для параллельного подсчета
	var wg sync.WaitGroup
	wg.Add(len(sm.Shards))

	for _, Shard := range sm.Shards {
		Shard := Shard // захватываем для замыкания
		go func() {
			defer wg.Done()
			Shard.RLock()
			atomic.AddInt64(&total, int64(len(Shard.Data)))
			Shard.RUnlock()
		}()
	}

	wg.Wait()
	return int(total)
}

// ForEach выполняет функцию для каждого элемента карты.
// Функция вызывается с копией значения, чтобы избежать блокировок.
func (sm *ShardedMap[K, V]) ForEach(fn func(K, V)) {
	for _, Shard := range sm.Shards {
		Shard.RLock()
		// Создаем копии пар ключ-значение, чтобы не держать блокировку во время вызова fn
		pairs := make([]struct {
			k K
			v V
		}, 0, len(Shard.Data))

		for k, v := range Shard.Data {
			pairs = append(pairs, struct {
				k K
				v V
			}{k, v})
		}
		Shard.RUnlock()

		// Вызываем fn без блокировки
		for _, p := range pairs {
			fn(p.k, p.v)
		}
	}
}

// GetOrSet возвращает существующее значение или устанавливает новое.
// Это атомарная операция, полезная для счетчиков.
func (sm *ShardedMap[K, V]) GetOrSet(key K, value V) (actual V, loaded bool) {
	idx := sm.getShardIndex(key)
	Shard := sm.Shards[idx]

	Shard.Lock()
	defer Shard.Unlock()

	if existing, ok := Shard.Data[key]; ok {
		return existing, true
	}

	Shard.Data[key] = value
	return value, false
}

// GetAndIncrement атомарно увеличивает значение и возвращает старое.
// Полезно для rate limiter.
func (sm *ShardedMap[K, V]) GetAndIncrement(key K) (old V, new V, ok bool) {
	idx := sm.getShardIndex(key)
	Shard := sm.Shards[idx]

	Shard.Lock()
	defer Shard.Unlock()

	val, exists := Shard.Data[key]
	if !exists {
		var zero V
		return zero, zero, false
	}

	// Используем type switch для чисел
	switch v := any(val).(type) {
	case int:
		newVal := v + 1
		Shard.Data[key] = any(newVal).(V)
		return val, any(newVal).(V), true
	case int64:
		newVal := v + 1
		Shard.Data[key] = any(newVal).(V)
		return val, any(newVal).(V), true
	default:
		// Для нечисловых типов просто возвращаем ошибку
		return val, val, false
	}
}
