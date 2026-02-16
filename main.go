package main

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"sharded-map/shardedmap"
)

// ShardedMap - здесь предполагается, что код из предыдущего ответа
// находится в отдельном пакете. Для демонстрации я включу упрощенную версию
// прямо в main.go, но в реальном проекте нужно импортировать пакет

func main() {
	// Демонстрация 1: Базовые операции
	fmt.Println("=== Демонстрация 1: Базовые операции ===")
	demoBasicOperations()

	fmt.Println("\n=== Демонстрация 2: Конкурентный доступ ===")
	demoConcurrentAccess()

	fmt.Println("\n=== Демонстрация 3: Rate Limiter симуляция ===")
	demoRateLimiter()

	fmt.Println("\n=== Демонстрация 4: Сравнение производительности ===")
	demoPerformanceComparison()
}

// demoBasicOperations показывает основные методы работы с картой
func demoBasicOperations() {
	// Создаем карту с 16 шардами
	sm := shardedmap.NewShardedMap[string, int](16)

	// Set операции
	sm.Set("user:1", 100)
	sm.Set("user:2", 200)
	sm.Set("user:3", 300)

	// Get операции
	if val, ok := sm.Get("user:1"); ok {
		fmt.Printf("user:1 = %d\n", val)
	}

	// GetOrSet - атомарная операция
	val, loaded := sm.GetOrSet("user:2", 999)
	if loaded {
		fmt.Printf("user:2 уже существовал, значение: %d\n", val)
	}

	// Получаем все ключи
	keys := sm.Keys()
	fmt.Printf("Все ключи: %v\n", keys)

	// Общее количество элементов
	fmt.Printf("Всего элементов: %d\n", sm.Len())

	// Итерация по всем элементам
	fmt.Println("Итерация по элементам:")
	sm.ForEach(func(k string, v int) {
		fmt.Printf("  %s -> %d\n", k, v)
	})

	// Удаление
	sm.Delete("user:2")
	fmt.Printf("После удаления user:2, элементов: %d\n", sm.Len())
}

// demoConcurrentAccess демонстрирует работу с множеством горутин
func demoConcurrentAccess() {
	sm := shardedmap.NewShardedMap[int, string](64)

	var wg sync.WaitGroup

	// Запускаем 100 горутин для записи
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < 100; j++ {
				key := workerID*1000 + j
				value := fmt.Sprintf("value-from-worker-%d", workerID)
				sm.Set(key, value)

				// Иногда читаем
				if j%10 == 0 {
					if val, ok := sm.Get(key); ok {
						_ = val // просто проверяем что ключ есть
					}
				}
			}
		}(i)
	}

	// Запускаем 10 горутин для чтения
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for range 50 {
				keys := sm.Keys()
				fmt.Printf("Текущее количество ключей: %d\n", len(keys))
				<-ticker.C
			}
		}()
	}

	wg.Wait()

	// Проверяем итоговое состояние
	fmt.Printf("Всего ключей после конкурентного доступа: %d\n", sm.Len())
	fmt.Printf("Распределение по шардам:\n")

	// Показываем распределение ключей по шардам
	for i, shard := range sm.Shards {
		shard.RLock()
		count := len(shard.Data)
		shard.RUnlock()

		if count > 0 {
			fmt.Printf("  Шард %d: %d ключей\n", i, count)
		}
	}
}

// demoRateLimiter симулирует работу rate limiter для API
func demoRateLimiter() {
	// Создаем rate limiter с 256 шардами для максимальной производительности
	limiter := shardedmap.NewShardedMap[string, *userRateLimit](256)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Симулируем 50 пользователей
	userIDs := make([]string, 50)
	for i := range userIDs {
		userIDs[i] = fmt.Sprintf("user-%d", i)
	}

	// Запускаем "нагрузку" от пользователей
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ { // 20 конкурентных воркеров
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Выбираем случайного пользователя
					userID := userIDs[rnd.Intn(len(userIDs))]

					// Проверяем лимиты
					limit, err := checkAndIncrement(limiter, userID, 100, time.Minute)
					if err != nil {
						fmt.Printf("[Воркер %d] %s: %v\n", workerID, userID, err)
					} else {
						fmt.Printf("[Воркер %d] %s: запрос разрешен (%d/%d)\n",
							workerID, userID, limit.Current, limit.Limit)
					}

					// Случайная задержка между запросами
					time.Sleep(time.Duration(rnd.Intn(50)) * time.Millisecond)
				}
			}
		}(i)
	}

	// Ждем 5 секунд и завершаем
	<-ctx.Done()
	wg.Wait()

	// Итоговая статистика
	fmt.Println("\nИтоговая статистика rate limiter:")
	limiter.ForEach(func(userID string, limit *userRateLimit) {
		fmt.Printf("  %s: %d/%d запросов\n", userID, limit.Current, limit.Limit)
	})
}

// userRateLimit представляет лимиты для пользователя
type userRateLimit struct {
	Current   int
	Limit     int
	ResetTime time.Time
}

// checkAndIncrement проверяет и увеличивает счетчик запросов
func checkAndIncrement(limiter *shardedmap.ShardedMap[string, *userRateLimit],
	userID string, maxRequests int, window time.Duration) (*userRateLimit, error) {

	// Пытаемся получить существующий лимит
	limit, _ := limiter.GetOrSet(userID, &userRateLimit{
		Current:   0,
		Limit:     maxRequests,
		ResetTime: time.Now().Add(window),
	})

	// Проверяем не истекло ли окно
	if time.Now().After(limit.ResetTime) {
		// Сбрасываем счетчик
		limit.Current = 1
		limit.ResetTime = time.Now().Add(window)
		limiter.Set(userID, limit)
		return limit, nil
	}

	// Проверяем лимит
	if limit.Current >= maxRequests {
		return limit, fmt.Errorf("rate limit exceeded (max: %d)", maxRequests)
	}

	// Увеличиваем счетчик
	limit.Current++
	return limit, nil
}

// demoPerformanceComparison сравнивает производительность с простой sync.RWMutex картой
func demoPerformanceComparison() {
	fmt.Println("Запуск бенчмарков...")

	// Тест 1: 1 шард (как обычный мьютекс)
	sm1 := shardedmap.NewShardedMap[int, int](1)
	benchConcurrent("1 шард (эмуляция одного мьютекса)", sm1, 100000)

	// Тест 2: 16 шардов
	sm16 := shardedmap.NewShardedMap[int, int](16)
	benchConcurrent("16 шардов", sm16, 100000)

	// Тест 3: 64 шарда
	sm64 := shardedmap.NewShardedMap[int, int](64)
	benchConcurrent("64 шарда", sm64, 100000)

	// Тест 4: 256 шардов
	sm256 := shardedmap.NewShardedMap[int, int](256)
	benchConcurrent("256 шардов", sm256, 100000)
}

// benchConcurrent выполняет конкурентный тест производительности
func benchConcurrent(name string, sm *shardedmap.ShardedMap[int, int], ops int) {
	runtime.GC()
	start := time.Now()

	var wg sync.WaitGroup

	// 8 конкурентных горутин
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

			for j := 0; j < ops/8; j++ {
				key := rnd.Intn(10000) // 10000 уникальных ключей

				// 90% операций чтения, 10% записи
				if rnd.Float64() < 0.9 {
					sm.Get(key)
				} else {
					sm.Set(key, j)
				}
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(start)
	fmt.Printf("%s: %v для %d операций (%.2f ops/sec)\n",
		name, elapsed, ops, float64(ops)/elapsed.Seconds())
}
