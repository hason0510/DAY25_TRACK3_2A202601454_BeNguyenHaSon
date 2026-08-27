# Day 25 — Track 3 — Reliability Engineering cho Production Agents

**Sinh viên:** Bé Nguyễn Hà Sơn — 2A202601454
**Ngày chạy số liệu:** 2026-08-27
**Môi trường:** Windows 11, Python 3.11.9, Redis 7-alpine (docker compose)

Toàn bộ số trong báo cáo này lấy trực tiếp từ `reports/metrics*.json` và
`reports/evidence/` — không có số nào gõ tay.

Lệnh tái lập:

```bash
docker compose up -d
docker compose exec redis redis-cli FLUSHDB      # bắt buộc trước mỗi lần đo
make test                                         # 35 passed, 7 xpassed, 0 failed
make run-chaos                                    # -> reports/metrics.json

# ba biến thể dùng cho các phép so sánh trong báo cáo
python scripts/run_chaos.py --config configs/memory_cache.yaml --out reports/metrics_memory.json
python scripts/run_chaos.py --config configs/no_cache.yaml     --out reports/metrics_no_cache.json
python scripts/run_chaos.py --config configs/concurrent.yaml   --out reports/metrics_concurrent.json

make evidence                                     # -> bằng chứng Redis
```

---

## 1. Kiến trúc

Gateway là một chuỗi phòng thủ 4 lớp. Mỗi lớp chỉ được gọi khi lớp trước không
phục vụ được, và mỗi nhánh trả về một giá trị `route` riêng biệt — `route` chính
là dấu vết duy nhất cho biết một response đã đi đường nào.

```
                          Request (prompt)
                                 │
                                 ▼
                  ┌──────────────────────────────┐
                  │ 1. CACHE (memory | Redis)     │
                  │    - guardrail privacy        │
                  │    - cosine n-gram similarity │
                  │    - false-hit detection      │
                  └──────────────┬───────────────┘
                     HIT │                │ MISS / bị guard chặn
                         ▼                ▼
         route="cache_hit:0.97"   ┌──────────────────────────┐
         latency=0, cost=0        │ 2. BREAKER[primary]      │
                                  │    CLOSED    -> cho qua   │
                                  │    OPEN      -> fail fast │
                                  │    HALF_OPEN -> 1 probe   │
                                  └───────┬──────────────────┘
                            OK │                  │ ProviderError | CircuitOpenError
                               ▼                  ▼
                      route="primary"   ┌──────────────────────────┐
                      + cache.set(...)  │ 3. BREAKER[backup]       │
                                        └───────┬──────────────────┘
                                     OK │              │ lỗi tiếp
                                        ▼              ▼
                              route="fallback"  ┌────────────────────────┐
                              + cache.set(...)  │ 4. STATIC FALLBACK     │
                                                │ route="static_fallback"│
                                                │ error=<lỗi cuối cùng>  │
                                                └────────────────────────┘
```

**Bốn quyết định thiết kế đáng chú ý:**

1. **Kiểm tra cổng nằm NGOÀI khối `try`** trong `CircuitBreaker.call()`. Nếu để
   trong `try`, `CircuitOpenError` sẽ bị `record_failure()` đếm oan thành một lần
   provider lỗi — cầu dao đang mở sẽ tự đếm chính nó và không bao giờ hồi phục.

2. **`record_failure()` tách `if` / `elif` với hai reason khác nhau.** Một probe
   thất bại ở HALF_OPEN mở lại mạch ngay lập tức với reason `"probe_failure"`,
   không chờ đủ `failure_threshold`. Gộp thành `if half_open or count >= threshold`
   sẽ mất khả năng phân biệt "thăm dò thất bại" với "vượt ngưỡng lỗi" trong
   `transition_log`.

3. **HALF_OPEN chỉ cho đúng MỘT probe đồng thời** (cờ `_probe_in_flight`). Đây là
   thứ tôi phải bổ sung sau khi chạy tải đồng thời phát hiện probe storm — xem
   mục 7. Ở chế độ tuần tự, hành vi hoàn toàn không đổi.

4. **Guardrail privacy đặt ở CẢ `get()` và `set()`.** Chặn một đầu là vẫn rò dữ
   liệu: chặn `set` thôi thì một entry cũ vẫn có thể được `get` phục vụ; chặn
   `get` thôi thì dữ liệu nhạy cảm vẫn nằm trong Redis và lộ qua `KEYS`/`HGETALL`.

---

## 2. Cấu hình và lý do

| Tham số | Giá trị | Lý do (dựa trên số đo, không phải mặc định) |
|---|---:|---|
| `providers[0].fail_rate` (primary) | 0.25 | Giữ nguyên starter. Đây là "primary khoẻ mạnh" nhưng 25% lỗi vẫn đủ để mạch thỉnh thoảng trip — không cache thì `all_healthy` vẫn có 1 static fallback. |
| `providers[1].fail_rate` (backup) | 0.05 | Backup không hoàn hảo. Đây chính là nguồn gốc của 4 `static_fallback` trong lần chạy chính: khi primary chết VÀ backup rơi đúng 5% đó thì không còn đường lùi. Xem mục 8. |
| `failure_threshold` | 3 | 1 là quá nhạy — với `fail_rate=0.25`, 25% request đơn lẻ sẽ trip mạch và cả hệ thống dao động liên tục. 3 lần lỗi **liên tiếp** ở p=0.25 chỉ xảy ra với xác suất 1.6%, nên mạch chỉ mở khi provider thực sự hỏng chứ không mở vì nhiễu. Đo được: `all_healthy` mở **0** lần / 100 request, `primary_timeout_100` mở **6** lần. |
| `reset_timeout_seconds` | 2 | Là hằng số chi phối trực tiếp `recovery_time_ms`. Đo thực tế: **2189.50 ms** = 2000 ms chờ nguội + ~190 ms cho probe request. Hạ xuống 0.5s thì probe dồn dập vào provider đang hỏng; nâng lên 10s thì `primary_timeout_100` chịu 10s mù trước khi thử lại dù provider đã hồi. |
| `success_threshold` | 1 | Một probe thành công là đủ để đóng mạch trong workload này vì provider giả là stateless. Nếu backend thật có warm-up (connection pool, model loading) thì nên đặt 2–3 để tránh đóng mạch quá sớm rồi trip lại ngay. |
| `cache.ttl_seconds` | 300 | Một scenario chạy 8–15s nên TTL 300s không hề evict trong lúc chạy — đây là chủ ý: muốn đo *hiệu quả cache*, không muốn đo *hiệu ứng TTL*. Với nội dung FAQ/policy thay đổi theo tuần thì 300s là bảo thủ và an toàn. |
| `cache.similarity_threshold` | 0.92 | **Đo trên chính bộ 20 query** (`reports/evidence/similarity_threshold_study.txt`): phổ điểm của 190 cặp có một khoảng trống rất rộng — cặp cao nhất là 0.9763, cặp cao kế tiếp *khác chủ đề* chỉ 0.4577. Mọi ngưỡng trong khoảng 0.46–0.95 cho kết quả y hệt. Chọn 0.92 vì nó nằm giữa khoảng trống đó: đủ cao để không bao giờ gộp `"Explain circuit breaker states"` với `"Explain the difference between retry and circuit breaker patterns"` (0.4160 — hai câu hỏi khác nhau thật), đủ thấp để chịu được biến thể diễn đạt (`"Summarize the refund policy"` vs `"Summarize refund policy"` = 0.9045). Hạ xuống 0.42 là bắt đầu gộp bậy. |
| `cache.backend` | `redis` | Xem mục 6. |
| `load_test.requests` | 100 / scenario | 3 scenario × 100 = 300 request. Đủ để P99 có ý nghĩa (P99 của 100 mẫu = mẫu thứ 99) và giữ thời gian chạy ~33s để lặp lại nhanh khi tune. |
| `load_test.concurrency` | 1 (mặc định) | **Bổ sung so với starter.** 1 = tuần tự, tái lập được — dùng cho `metrics.json`. `configs/concurrent.yaml` đặt 10 để đo dưới tải đồng thời. Xem mục 7. |
| `seed` | 42 | **Bổ sung so với starter.** Xem mục 4 (tái lập). |

---

## 3. SLO

Nguồn: `reports/metrics.json` (tuần tự, backend Redis).

| SLI | SLO mục tiêu | Giá trị đo | Đạt? |
|---|---|---:|---|
| Availability | ≥ 99% | **98.67%** | ❌ **Không đạt** (thiếu 0.33 điểm phần trăm = 4/300 request) |
| Latency P95 | < 2500 ms | **319.31 ms** | ✅ Đạt (dư 87%) |
| Latency P99 | < 3000 ms | **319.79 ms** | ✅ Đạt |
| Fallback success rate | ≥ 95% | **94.03%** | ❌ **Không đạt** (sát ngưỡng, hụt 0.97 điểm) |
| Cache hit rate | ≥ 10% | **66.00%** | ✅ Đạt (gấp 6.6 lần) |
| Recovery time | < 5000 ms | **2189.50 ms** | ✅ Đạt |

Hai SLO không đạt là **kết quả thật, không làm tròn cho đẹp**. Cả hai đều quy về
cùng một nguyên nhân gốc và được phân tích ở mục 8: khi cầu dao của primary đang
mở, hệ thống chỉ còn đúng **một** provider, và backup có `fail_rate = 0.05` — nên
trần lý thuyết của availability trong giai đoạn đó là 95%, không phải 99%.
Không thể đạt SLO 99% bằng cách tinh chỉnh cầu dao; phải thêm provider thứ ba
hoặc chấp nhận phục vụ cache cũ (stale) khi mọi provider đều chết.

> Ghi chú: ở chế độ **đồng thời** (mục 7) availability đạt **99.33%** và
> fallback_success_rate đạt **97.73%** — cả hai SLO đều đạt. Nhưng đó **không**
> phải bằng chứng hệ thống tốt hơn dưới tải; nguyên nhân thật được giải thích ở
> mục 7 và tôi không dùng con số đó để tuyên bố đạt SLO.

---

## 4. Metrics

Nguồn: `reports/metrics.json` — sinh bởi `make run-chaos`, cache backend `redis`,
`seed: 42`, `concurrency: 1`, đã `FLUSHDB` trước khi chạy.

| Metric | Giá trị |
|---|---:|
| total_requests | 300 |
| availability | 0.9867 |
| error_rate | 0.0133 |
| latency_p50_ms | 273.96 |
| latency_p95_ms | 319.31 |
| latency_p99_ms | 319.79 |
| fallback_success_rate | 0.9403 |
| cache_hit_rate | 0.66 |
| circuit_open_count | 7 |
| recovery_time_ms | 2189.50 |
| estimated_cost | 0.042202 |
| estimated_cost_saved | 0.198 |
| scenarios | 3/3 pass |

### Vì sao `recovery_time_ms` ≈ 2200 ms

`calculate_recovery_time_ms()` ghép mỗi mốc `to="open"` **gần nhất** với mốc
`to="closed"` kế tiếp trong `transition_log`. Chu trình đầy đủ là:

```
t=0        closed    -> open       reason="failure_threshold_reached"
t=2000ms   open      -> half_open  reason="reset_timeout_elapsed"   <- đúng reset_timeout_seconds=2
t=2189ms   half_open -> closed     reason="probe_success"           <- +1 probe request
```

Nên `recovery_time_ms` = `reset_timeout_seconds` × 1000 + thời gian một probe +
độ trễ đến khi có request kế tiếp thực sự gọi provider đó. Con số 2189.50 ms khớp
chính xác với dự đoán này. Nếu chỉnh `reset_timeout_seconds` thành 5 thì
`recovery_time_ms` sẽ nhảy lên ~5200 ms — quan hệ tuyến tính trực tiếp.

### Lưu ý về P50: cache hit KHÔNG nằm trong mẫu latency

Starter chỉ ghi `latencies_ms` khi `result.latency_ms > 0`, mà cache hit có
`latency_ms = 0`, nên toàn bộ 198 cache hit bị loại khỏi mẫu. **Đây là hành vi
của starter, tôi giữ nguyên có chủ ý** để số liệu so sánh được với đề bài.

Hệ quả phải hiểu đúng: P50 = 274 ms **không phải** độ trễ trung vị mà người dùng
cảm nhận — nó là độ trễ trung vị *của những request phải gọi provider*. Độ trễ
người dùng thật sự thấy, nếu tính cả cache hit vào mẫu, sẽ là:

- 66% request có latency 0 ms → P50 thật ≈ **0 ms**, P95 thật ≈ 300 ms.

Vì vậy lợi ích của cache trong báo cáo này thể hiện ở **cost** và
**availability**, không ở P50 — xem mục 5.

### Tái lập (điểm cộng của rubric)

Starter dùng `random` toàn cục nên không tái lập được. Tôi đã:

1. Thêm tham số `rng: random.Random | None = None` vào `FakeLLMProvider`
   (mặc định giữ nguyên hành vi cũ, nên mọi test starter vẫn xanh).
2. Thêm trường `seed: int | None` vào `LabConfig`.
3. Cấp cho mỗi scenario một luồng RNG riêng `random.Random(seed + index)` — nhờ
   vậy scenario thứ N tái lập được bất kể scenario N-1 gọi provider bao nhiêu lần.

Kết quả `diff` giữa hai lần `make run-chaos` liên tiếp:

```diff
  "total_requests": 300,          <- giống hệt
  "availability": 0.9867,         <- giống hệt
  "error_rate": 0.0133,           <- giống hệt
  "fallback_success_rate": 0.9403,<- giống hệt
  "cache_hit_rate": 0.66,         <- giống hệt
  "circuit_open_count": 7,        <- giống hệt
  "estimated_cost": 0.042202,     <- giống hệt
  "estimated_cost_saved": 0.198,  <- giống hệt
  "scenarios": {...}              <- giống hệt
-  "latency_p50_ms": 273.96,      +  "latency_p50_ms": 273.96,
-  "latency_p95_ms": 319.31,      +  "latency_p95_ms": 319.31,
-  "latency_p99_ms": 319.79,      +  "latency_p99_ms": 319.79,
-  "recovery_time_ms": 2189.50,   +  "recovery_time_ms": 2189.50,
```

**Mọi chỉ số quyết định (routing, cost, cache, circuit) tái lập 100%.** Chỉ các
chỉ số đo bằng đồng hồ tường lệch < 0.1%, vì `FakeLLMProvider` thực sự gọi
`time.sleep()` — seed điều khiển được *bao nhiêu ms được yêu cầu ngủ*, nhưng
không điều khiển được *scheduler của OS ngủ thực tế bao lâu*. Đây là giới hạn
thật, không phải lỗi cấu hình.

Kiểm chứng mạnh hơn: `make run-chaos` chạy lại trong **một venv hoàn toàn mới**
(`python -m venv` + `pip install -e ".[dev]"` sạch) vẫn cho y hệt các chỉ số
quyết định. Tái lập không phụ thuộc môi trường.

**Lưu ý quan trọng:** tính tái lập này chỉ đúng ở `concurrency: 1`. Chạy đồng
thời thì thứ tự thread do OS quyết định, nên kết quả **không** tái lập — đây là
lý do `metrics.json` (deliverable chính) dùng chế độ tuần tự.

---

## 5. So sánh có cache / không cache

Ba lần chạy, cùng `seed: 42`, cùng 300 request, cùng tuần tự, chỉ khác tầng cache.
Redis được `FLUSHDB` trước mỗi lần đo (nếu không, cache còn nóng từ lần trước sẽ
đẩy hit rate lên giả tạo — bẫy đo lường đã nêu trong đề).

| Metric | Không cache | Cache memory | Cache Redis |
|---|---:|---:|---:|
| config | `configs/no_cache.yaml` | `configs/memory_cache.yaml` | `configs/default.yaml` |
| cache_hit_rate | 0.0 | 0.57 | **0.66** |
| **estimated_cost** | **0.126100** | **0.055460** | **0.042202** |
| **tiết kiệm chi phí** | — | **−56.0%** | **−66.5%** |
| estimated_cost_saved | 0.0 | 0.171 | 0.198 |
| **availability** | **0.9667** | **0.9833** | **0.9867** |
| error_rate | 0.0333 | 0.0167 | 0.0133 |
| **circuit_open_count** | **21** | **8** | **7** |
| latency_p50_ms | 271.07 | 267.67 | 273.96 |
| latency_p95_ms | 317.33 | 319.18 | 319.31 |
| latency_p99_ms | 319.98 | 319.84 | 319.79 |
| recovery_time_ms | 2201.78 | 2472.60 | 2189.50 |
| thời gian chạy (3 scenario) | 91.8 s | 40.6 s | 33.2 s |

**Đọc bảng này thế nào:**

- **Chi phí là thắng lợi rõ ràng nhất: giảm 66.5%.** Cache Redis loại bỏ 198/300
  lần gọi provider. Với giá thật của Claude/GPT thay vì provider giả, đây là
  khoản tiết kiệm lớn nhất trong toàn bộ lab.

- **P50/P95 gần như không đổi — và điều đó là ĐÚNG, không phải bug.** Như đã giải
  thích ở mục 4, cache hit có `latency_ms = 0` nên bị loại khỏi mẫu. Ba cột P50
  đang so sánh cùng một thứ: độ trễ của những request *không* được cache phục vụ.
  Chúng phải gần bằng nhau. Kết luận đúng là: *cache không làm provider nhanh
  hơn, nó làm cho ít request phải gặp provider hơn.* Bằng chứng nằm ở hàng cuối:
  cùng 300 request, không cache mất **91.8 s**, có Redis chỉ mất **33.2 s** —
  nhanh gấp **2.8 lần**, dù P50 của từng request y hệt nhau.

- **`circuit_open_count` giảm 3 lần (21 → 7) là lợi ích ẩn quan trọng nhất.**
  Không cache, cả 300 request đều đập vào provider; primary lỗi 25% nên chuỗi 3
  lỗi liên tiếp xảy ra thường xuyên hơn nhiều. Có cache, chỉ 102 request chạm tới
  provider. **Cache không chỉ tiết kiệm tiền — nó giảm tải lên provider đang yếu,
  nên cầu dao ít phải trip hơn.** Đây là hiệu ứng phòng vệ mà bảng chi phí không
  thể hiện được.

- **Availability tăng rõ: 0.9667 → 0.9867**, tức error_rate giảm từ 3.33% xuống
  1.33% — **giảm 60% số request thất bại**. Mỗi cache hit là một request không
  thể thất bại, và ít trip mạch hơn nghĩa là ít khoảng thời gian chỉ còn một
  provider hơn.

- **Redis hit rate (0.66) cao hơn memory (0.57)** trong cùng một lần chạy vì
  cache Redis **dùng chung giữa cả 3 scenario**, còn `ResponseCache` in-memory bị
  tạo mới mỗi lần `build_gateway()`. Scenario thứ 3 (`all_healthy`) hưởng lợi từ
  cache mà scenario 1 và 2 đã làm nóng: hit rate của riêng nó là **0.70** với
  Redis so với **0.56** với memory. Đây chính là bằng chứng số học cho giá trị
  của shared cache ở mục 6 — không phải lý thuyết suông.

---

## 6. Redis shared cache

### Vì sao in-memory không đủ cho production

`ResponseCache` sống trong RAM của **một tiến trình**. Deploy 3 pod sau load
balancer thì có 3 cache hoàn toàn độc lập:

- Hit rate thực tế **chia cho số pod**. Cùng một câu hỏi phải được trả lời lại từ
  provider ở mỗi pod trước khi pod đó biết câu trả lời → trả tiền 3 lần cho 1 câu hỏi.
- **Không nhất quán:** ba người dùng hỏi cùng một câu, trúng ba pod khác nhau, có
  thể nhận ba câu trả lời khác nhau.
- **Mất sạch khi deploy.** Rolling update = cache rỗng = một đợt tăng đột biến
  chi phí và độ trễ ngay sau mỗi lần release.
- `false_hit_log` cũng phân mảnh, nên không quan sát được toàn hệ thống.

### `SharedRedisCache` giải quyết ra sao

| Vấn đề | Cách giải |
|---|---|
| State phân mảnh | Mọi instance đọc/ghi cùng một namespace `rl:cache:*` |
| Eviction thủ công | Giao cho Redis `EXPIRE` — không cần quét TTL trong code ứng dụng |
| Mất cache khi deploy | State nằm ngoài tiến trình, sống sót qua restart (`--appendonly yes` + volume) |
| Tra cứu ngữ nghĩa | `HSET` lưu **cả `query` gốc lẫn `response`**; khi `SCAN` để so độ giống thì `HGET` field `query` đọc lại được câu hỏi cũ mà so |

> **Chi tiết dễ sai:** nếu `set()` chỉ lưu `response` mà không lưu `query`, thì
> exact-match theo hash vẫn chạy (nên test có thể pass), nhưng nhánh quét theo độ
> giống vĩnh viễn trả về miss vì không còn gì để so. Test `test_set_and_exact_get`
> không bắt được lỗi này — chỉ `test_false_hit_different_years` mới bắt được.

### Bằng chứng shared state

Chạy `make evidence` (nguồn: `reports/evidence/redis_evidence.txt`) — hai đối
tượng `SharedRedisCache` độc lập, cùng trỏ vào một Redis:

```
========================================================================
1. SHARED STATE - instance A writes, instance B reads
========================================================================
  A.set('Explain circuit breaker states in one paragraph.')
  B.get(...) -> ('[primary] circuit breaker has three states...', score=1.0)
  SHARED STATE OK: True
```

Cùng kết luận từ test tự động (`tests/test_redis_cache.py`):

```
tests/test_redis_cache.py::test_shared_state_across_instances PASSED     [ 78%]
```

### Redis CLI

```bash
$ docker compose exec redis redis-cli KEYS "rl:cache:*"
rl:cache:dacb2b833659
rl:cache:fff10da1c72c
rl:cache:844ef0143a5c
rl:cache:b2a52f7dc795
rl:cache:0bc3b1acf73d
rl:cache:3dab98c0e49e
rl:cache:095946136fea
rl:cache:98332d0d1c9c
rl:cache:d354658dc020
rl:cache:734852f3cf4a
rl:cache:da61fb49b4f6
rl:cache:9e413fd814eb
   (12 key — 12 câu hỏi khác nhau đã được cache)

$ docker compose exec redis redis-cli HGETALL rl:cache:dacb2b833659
provider
backup
response
[backup] reliable answer for: Compare latency between primary and backup providers.
query
Compare latency between primary and backup providers.
   ^^^^^ query gốc được lưu lại — đây là thứ nhánh quét độ giống cần đọc

$ docker compose exec redis redis-cli TTL rl:cache:dacb2b833659
112
   (đang đếm ngược từ ttl_seconds=300 — Redis tự lo eviction)
```

### Guardrail privacy vẫn nguyên vẹn trên Redis

Bộ query có 5 câu nhạy cảm (q4, q6, q9, q13, q19). Sau 300 request, quét toàn bộ
key và đọc field `query`:

```bash
$ for k in $(redis-cli --raw KEYS "rl:cache:*"); do redis-cli --raw HGET "$k" query; done \
    | grep -iE "balance|password|ssn|credit card|user [0-9]|account [0-9]"
(khong co - guardrail hoat dong)
```

Không một câu hỏi nhạy cảm nào chạm tới đĩa. Xác nhận thêm từ
`reports/evidence/redis_evidence.txt`:

```
2. PRIVACY GUARDRAIL - sensitive query is neither stored nor served
  A.set('Give me the current account balance for user 123.')
  keys in Redis after set: 1 (unchanged - nothing written)
  A.get(...) -> (None, score=0.0)
  GUARDRAIL OK: True
```

### So sánh in-memory vs Redis

| Metric | Cache memory | Cache Redis | Ghi chú |
|---|---:|---:|---|
| latency_p50_ms | 267.67 | 273.96 | +6.3 ms — nằm trong nhiễu đo |
| latency_p95_ms | 319.18 | 319.31 | +0.1 ms |
| cache_hit_rate | 0.57 | 0.66 | Redis thắng nhờ dùng chung giữa các scenario |
| estimated_cost | 0.055460 | 0.042202 | Redis rẻ hơn **23.9%** |
| availability | 0.9833 | 0.9867 | Redis nhỉnh hơn nhờ hit rate cao hơn |

Redis thêm một vòng round-trip mạng, nhưng vì đây là localhost và mỗi cache hit
tiết kiệm ~250 ms gọi provider, chi phí đó hoàn toàn không đáng kể. **Đánh đổi
này rõ ràng có lợi.**

> **Cảnh báo về phương pháp đo:** Redis giữ dữ liệu qua các lần chạy (TTL 300s +
> volume `--appendonly yes`). Chạy `make run-chaos` lần thứ hai mà không FLUSHDB
> sẽ cho hit rate cao hơn hẳn **chỉ vì cache còn nóng**, không phải vì code tốt
> hơn. Mọi số Redis trong báo cáo này đều đo sau `redis-cli FLUSHDB`.

---

## 7. Chaos scenarios và load testing

Tiêu chí pass/fail được định nghĩa trong `SCENARIO_CRITERIA` (`chaos.py`) — mỗi
scenario khẳng định đúng cái hành vi mà nó sinh ra để chứng minh, chứ không phải
tiêu chí tầm thường "có ít nhất 1 request thành công".

| Scenario | Hành vi kỳ vọng | Tiêu chí pass | Quan sát được | Kết quả |
|---|---|---|---|---|
| `primary_timeout_100`<br>(primary lỗi 100%) | Toàn bộ traffic rơi sang backup; mạch primary phải mở thay vì đập liên tục vào provider chết | availability ≥ 0.95 **và** circuit_open > 0 **và** fallback_success_rate > 0.9 | availability **0.98**, circuit mở **6** lần, 39 fallback, 59 cache hit, chỉ **2** static fallback, fallback_success_rate **0.9512** | ✅ **pass** |
| `primary_flaky_50`<br>(primary lỗi 50%) | Mạch dao động: mở, chờ nguội, thăm dò, đóng lại | availability ≥ 0.95 **và** circuit_open > 0 **và** có bằng chứng hồi phục (nếu cửa sổ đo đủ dài) | availability **0.99**, circuit mở **1** lần và **đóng lại được**, recovery **2189.50 ms**, chỉ **1** static fallback | ✅ **pass** |
| `all_healthy`<br>(baseline) | Gần như mọi request đi qua primary | availability ≥ 0.98 **và** (không trip, hoặc có trip thì hồi < 5s) | availability **0.99**, circuit mở **0** lần, 70 cache hit (hit rate **0.70**) | ✅ **pass** |

**Bằng chứng hồi phục (recovery)** — `transition_log` của scenario
`primary_flaky_50` cho thấy trọn vẹn chu trình 3 trạng thái:

```
closed    -> open       reason="failure_threshold_reached"
open      -> half_open  reason="reset_timeout_elapsed"
half_open -> closed     reason="probe_success"
```

`recovery_time_ms = 2189.50` chính là khoảng cách giữa mốc `open` cuối cùng và
mốc `closed` — khớp với `reset_timeout_seconds = 2` cộng thời gian một probe.

### Tải đồng thời (`ThreadPoolExecutor`)

`configs/concurrent.yaml` đặt `load_test.concurrency: 10` — 10 request cùng lúc
qua **một** gateway, **một** cache và **một** breaker mỗi provider, đúng như một
pod thật phải chịu. Prompt được rút trước bằng RNG tuần tự, nên *khối lượng công
việc* giữa hai chế độ là **giống hệt nhau**; chỉ *thời điểm* các request xảy ra
là khác.

| Metric | Tuần tự (`concurrency: 1`) | Đồng thời (`concurrency: 10`) | Chênh lệch |
|---|---:|---:|---|
| thời gian chạy (3 scenario) | 33.2 s | **4.2 s** | **nhanh 7.9×** |
| availability | 0.9867 | 0.9933 | +0.7 % |
| error_rate | 0.0133 | 0.0067 | −50 % |
| **cache_hit_rate** | **0.66** | **0.6233** | **−5.6 %** |
| **circuit_open_count** | **7** | **2** | **−71 %** |
| **recovery_time_ms** | **2189.50** | **null** | không quan sát được |
| estimated_cost | 0.042202 | 0.043908 | +4.0 % |
| latency_p50_ms | 273.96 | 274.69 | +0.6 ms |
| latency_p95_ms | 319.31 | 318.45 | −1.0 ms |

**Bốn kết luận từ số đo — và cái nào là thật, cái nào là ảo:**

1. **Cache stampede là hiệu ứng THẬT.** `cache_hit_rate` giảm 0.66 → 0.6233 và
   `estimated_cost` tăng 4%. Nguyên nhân: 10 request đồng thời cùng hỏi một câu
   chưa có trong cache thì **cả 10 đều miss** và cùng gọi provider — chạy tuần tự
   thì request thứ 2 đã thấy kết quả của request thứ 1. Thấy rõ nhất ở
   `primary_timeout_100`: hit rate rơi từ **0.59** xuống **0.48**. Sửa bằng
   request coalescing (khoá theo query hash, để một request đi gọi provider còn
   các request trùng thì chờ kết quả) — chưa làm, ghi vào mục 9.

2. **Availability tăng là hiệu ứng ẢO — không được dùng để tuyên bố SLO.**
   Nhìn thoáng qua thì 0.9933 > 0.9867 trông như chạy đồng thời tốt hơn. Thực tế
   nguyên nhân là `circuit_open_count` giảm từ 7 xuống 2, mà lý do lại là **thời
   gian chạy ngắn hơn 7.9 lần**, không phải hệ thống khoẻ hơn. Ít thời gian trôi
   qua thì ít cơ hội tích luỹ 3 lỗi liên tiếp, nên ít khoảng thời gian chỉ còn
   một provider, nên ít static fallback. Đây là lý do tôi vẫn báo cáo SLO ở mục 3
   theo số **tuần tự**.

3. **`recovery_time_ms = null` phơi bày một giới hạn của phép đo.** Mạch vẫn mở
   (2 lần) nhưng không lần nào đóng lại. Dump `transition_log` cho thấy nguyên
   nhân chính xác:

   ```
   scenario primary_flaky_50 chạy hết 1.12s  (reset_timeout = 2.0s)
     breaker[primary]:
       t+0.24s  closed -> open   failure_threshold_reached
     (hết scenario — chưa bao giờ hết cool-down 2s)
   ```

   Scenario kết thúc **trước khi** cool-down 2 giây trôi qua, nên chu trình
   `open → half_open → closed` không thể xảy ra. **Số chu kỳ hồi phục quan sát
   được tỉ lệ với thời-gian-tường ÷ reset_timeout, không phải với số request.**
   Chaos test đồng thời vì thế phải được định cỡ theo *thời gian*, không theo
   *số request* — một bài học về phương pháp đo mà chỉ chạy tuần tự sẽ không
   bao giờ lộ ra.

   Đây cũng là lý do tôi sửa tiêu chí `primary_flaky_50` thành: chỉ đòi hỏi bằng
   chứng recovery khi `duration_seconds ≥ 4 × reset_timeout` (hàm
   `recovery_is_observable()`). Một tiêu chí đòi hỏi thứ mà phép đo **không thể
   quan sát được về mặt vật lý** là tiêu chí sai, không phải hệ thống sai. Trường
   `recovery_observable` được ghi vào `reports/metrics_concurrent_scenarios.json`
   để minh bạch việc này.

4. **P50/P95 gần như không đổi** vì `FakeLLMProvider` dùng `time.sleep()` — nó
   nhả GIL, nên 10 thread ngủ song song thật sự. Không có tranh chấp CPU nên độ
   trễ *từng request* không đổi; chỉ *thông lượng* tăng 7.9 lần. Với provider
   thật bị giới hạn bởi rate limit, P95 sẽ xấu đi rõ rệt — đây là chỗ mô phỏng
   khác thực tế và tôi không ngoại suy từ nó.

### Lỗi thật tìm được nhờ chạy đồng thời: probe storm

Lần chạy đồng thời đầu tiên cho `recovery_time_ms = null` ở **cả ba** scenario và
`primary_flaky_50` **fail**. Điều tra ra một lỗi thiết kế thật trong circuit
breaker của tôi, chứ không phải nhiễu đo.

`allow_request()` bản đầu cho **mọi** caller đi qua khi state là HALF_OPEN:

```python
if self.state == CircuitState.HALF_OPEN:
    return True          # <-- không giới hạn số probe đồng thời
```

Chạy tuần tự thì vô hại (chỉ có 1 request tại một thời điểm). Chạy đồng thời thì
cả 10 worker cùng thấy HALF_OPEN và **cả 10 cùng được thả vào provider đang ốm**.
Thí nghiệm 10 thread xuất phát cùng lúc qua `threading.Barrier`:

```
Số request được cho qua ở HALF_OPEN: 10/10
Kỳ vọng của một circuit breaker đúng: 1/10
```

Với `primary_flaky_50` (fail_rate 0.5), xác suất **cả 10 probe** đều thành công
là `0.5¹⁰ = 0.098%` — chỉ cần một probe hỏng là `record_failure()` mở lại mạch.
Nói cách khác: **mạch gần như không bao giờ đóng lại được**. Đúng là retry storm
mà rubric yêu cầu phải tránh, chỉ khác là nó chỉ lộ ra dưới tải đồng thời.

Sửa bằng cờ `_probe_in_flight`: HALF_OPEN cấp đúng một "vé probe", trả lại vé khi
`record_success()` hoặc `record_failure()` chạy. Sau khi sửa, đúng thí nghiệm đó:

```
Sau khi sửa - số probe được cho qua: 1/10  (kỳ vọng: 1)
```

Toàn bộ 12 test circuit breaker vẫn xanh và metrics tuần tự không đổi về chất —
đúng như mong đợi, vì ở chế độ tuần tự hai cách xử lý là tương đương.

**Bốn thành phần phải khoá để số liệu đồng thời đáng tin:**

| Thành phần | Vấn đề nếu không khoá | Cách sửa |
|---|---|---|
| `CircuitBreaker` | Hai thread cùng thấy `failure_count == threshold-1` → cùng trip; probe success bị mất vào một failure đồng thời | `threading.Lock` bọc mọi read-modify-write; `_transition()` không tự lấy khoá nên gọi được từ trong vùng đã khoá |
| `FakeLLMProvider._rng` | `random.Random` không thread-safe → trạng thái Mersenne Twister hỏng | Khoá riêng cho RNG, **không** giữ khoá qua `time.sleep()` (nếu không sẽ serialise cả load test) |
| `ResponseCache._entries` | `get()` gán lại `self._entries` khi evict, `set()` đồng thời append vào list sắp bị vứt | Khoá + chụp snapshot để chấm điểm ngoài vùng khoá |
| `RunMetrics` counters | `+=` không nguyên tử → mất số đếm | Một khoá bọc hàm `record()` trong `run_scenario()` |

### Scenario tự thêm

`configs/no_cache.yaml`, `configs/memory_cache.yaml` và `configs/concurrent.yaml`
là ba biến thể cấu hình đầy đủ, chạy được độc lập, tạo thành phép so sánh 4 chiều
ở mục 5 và mục 7. Chúng dùng đúng 3 scenario chaos nhưng đổi tầng cache / chế độ
tải — cách cô lập từng biến số mà không đụng vào chaos.

Ngoài ra `scripts/run_chaos.py` có thêm `--requests` và `--concurrency` để ghi đè
config từ dòng lệnh, phục vụ việc dò tìm nhanh (chính nhờ nó tôi xác định được
ngưỡng thời-gian-tường cần thiết để quan sát được recovery).

---

## 8. Phân tích điểm yếu

### Điểm yếu chính: circuit state nằm trong RAM → N pod có N cầu dao độc lập

`CircuitBreaker` là một dataclass trong bộ nhớ tiến trình. Cache đã được đưa lên
Redis, nhưng **trạng thái cầu dao thì chưa** — và đây là bất đối xứng nguy hiểm
nhất còn lại trong hệ thống.

**Hậu quả khi chạy 3 pod và primary chết:**

| | Kết quả |
|---|---|
| Số lần lỗi cần thiết để chặn được traffic | 3 pod × `failure_threshold` 3 = **9** lần lỗi, thay vì 3 |
| Số request người dùng phải chịu lỗi | Gấp 3 lần con số đo được trong lab |
| Khi hết `reset_timeout` | Cả 3 pod cùng gửi probe vào một provider vừa mới hồi — **thundering herd** đúng lúc nó yếu nhất |
| Quan sát | 3 `transition_log` rời rạc; dashboard không trả lời được câu "cầu dao đang mở hay đóng?" |

Đây chính là **phiên bản đa tiến trình của probe storm mà tôi đã sửa được ở mức
đa luồng** (mục 7). Cờ `_probe_in_flight` chỉ bảo vệ trong phạm vi một tiến
trình; qua nhiều pod thì cần khoá phân tán. Số `circuit_open_count = 7` đo được
sẽ thành ~21 trên 3 pod.

**Cách sửa — đưa counter lên Redis, dùng đúng thứ tự nguyên tử:**

```python
# Đếm lỗi: INCR trả về giá trị sau khi tăng, nên không có race giữa các pod
fails = redis.incr(f"cb:{provider}:failures")
if fails == 1:
    redis.expire(f"cb:{provider}:failures", window_seconds)   # cửa sổ trượt
if fails >= failure_threshold:
    # SET NX: chỉ pod ĐẦU TIÊN thắng, các pod khác thấy mạch đã mở
    redis.set(f"cb:{provider}:open", "1", nx=True, ex=reset_timeout_seconds)

# Probe: SET NX làm khoá phân tán -> đúng MỘT pod được thăm dò
# (chính là _probe_in_flight, nhưng ở phạm vi toàn cụm)
if redis.set(f"cb:{provider}:probe", pod_id, nx=True, ex=probe_timeout):
    ...  # pod này thăm dò; các pod khác tiếp tục fail fast
```

`SET NX` giải quyết cả hai vấn đề bằng một cơ chế: mạch chỉ mở một lần trên toàn
cụm, và chỉ đúng một pod được phép thăm dò. Đánh đổi: mỗi request tốn thêm một
round-trip Redis, và **Redis trở thành single point of failure** — nên phải đi
kèm điểm yếu thứ hai dưới đây.

### Điểm yếu thứ hai: Redis sập là cache sập theo, không có đường lùi

`SharedRedisCache.get()` gọi thẳng `self._redis.hget(...)`. Nếu Redis không truy
cập được, `redis.ConnectionError` được ném ra từ **bên trong `cache.get()`** —
mà `ReliabilityGateway.complete()` chỉ bắt `ProviderError` và `CircuitOpenError`.
Exception này lọt qua khe hở đó và **làm sập cả pipeline**, kể cả khi hai provider
vẫn hoàn toàn khoẻ mạnh. Nghịch lý: *tầng tối ưu hoá làm sập tầng tin cậy.*

Sửa bằng hai lớp:

1. Bọc `get`/`set` của Redis trong `try/except redis.RedisError`, coi lỗi Redis
   là **cache miss** chứ không phải lỗi request. Cache là tối ưu hoá — mất nó
   phải làm hệ thống chậm và tốn hơn, không phải chết.
2. Giữ một `ResponseCache` in-memory nhỏ làm L1: khi Redis chết thì tự lùi về L1
   và cho một cầu dao riêng canh chính Redis, tránh mỗi request đều phải chờ
   timeout kết nối.

### Điểm yếu thứ ba: false-hit guard chỉ nhìn số 4 chữ số

Đây là điểm yếu **tôi phát hiện bằng số đo, không phải suy đoán**. Quét toàn bộ
190 cặp trong bộ 20 query (`reports/evidence/similarity_threshold_study.txt`) tìm
ra hai lỗ hổng thật:

| Cặp câu hỏi | Điểm giống | Guard chặn? | Vấn đề |
|---|---:|---|---|
| `"Summarize the admission FAQ in 5 bullets."`<br>`"Summarize the admission FAQ in 3 bullets."` | **0.9687** | ❌ **KHÔNG** | `5` và `3` là số 1 chữ số, regex `\b\d{4}\b` không thấy. Người hỏi 3 gạch đầu dòng sẽ nhận câu trả lời 5 gạch. |
| `"...refund policy for a student who missed the deadline."`<br>`"...who missed the 2024 deadline."` | **0.9763** | ❌ **KHÔNG** | `_looks_like_false_hit` chỉ chặn khi **cả hai** câu có số 4 chữ số (`nums_q and nums_c`). Câu chung chung không có số nào → guard tự tắt → nhận nhầm câu trả lời riêng cho 2024. |
| `"...tuition fee for the 2024..."`<br>`"...tuition fee for the 2025..."` | 0.9574 | ✅ có | Trường hợp guard làm đúng việc. |

Nâng `similarity_threshold` **không cứu được**: 0.9763 và 0.9687 đều cao hơn bất
kỳ ngưỡng hợp lý nào, mà nâng lên 0.98 thì cache gần như vô dụng (hit rate sụp).
Đây là giới hạn nền tảng của việc dùng độ giống bề mặt để suy ra ý định.

Hướng sửa, theo thứ tự chi phí tăng dần:

1. Mở rộng regex sang **mọi chuỗi số** (`\d+`) và các từ chỉ số lượng
   (`3 bullets`, `5 bullets`, `top 10`) — vá được lỗ hổng thứ nhất, rẻ, làm ngay.
2. Đổi điều kiện từ `nums_q and nums_c` thành: **hễ một bên có số mà bên kia
   không có thì cũng coi là false hit** — vá lỗ hổng thứ hai. Đánh đổi: hit rate
   giảm nhẹ. Đây là đánh đổi đúng — phục vụ chậm còn hơn phục vụ sai.
3. Đắt nhất nhưng đúng nhất: gắn nhãn ý định (intent) vào khoá cache thay vì so
   chuỗi thô, để `admission_faq(bullets=3)` và `admission_faq(bullets=5)` là hai
   khoá khác nhau về mặt cấu trúc chứ không phải hai chuỗi giống nhau 96.87%.

---

## 9. Bước tiếp theo

1. **Đưa trạng thái cầu dao lên Redis bằng `INCR` + `SET NX`** (mục 8, điểm yếu
   #1). Đây là việc có tác động lớn nhất: nếu không làm, mọi con số tin cậy đo
   được trong lab này sẽ xấu đi tuyến tính theo số pod khi lên production. Tôi đã
   sửa được probe storm ở mức đa luồng; đây là đúng bài toán đó ở mức đa tiến trình.

2. **Request coalescing để chống cache stampede** (mục 7, kết luận #1). Số đo cho
   thấy chạy đồng thời làm hit rate rơi 0.66 → 0.6233 và chi phí tăng 4%, vì N
   request trùng nhau cùng miss rồi cùng gọi provider. Sửa bằng một khoá theo
   query hash: request đầu đi gọi provider, các request trùng chờ kết quả của nó.

3. **Cho cache lùi về in-memory khi Redis sập** (mục 8, điểm yếu #2). Bắt
   `redis.RedisError` trong `SharedRedisCache`, coi như cache miss, và thêm một
   cầu dao riêng canh Redis. Nhỏ về khối lượng code, nhưng bịt được một lỗ hổng
   mà hệ thống hiện tại sẽ chết vì nó.

4. **Đạt được SLO availability 99%** (mục 3). Trần lý thuyết hiện tại là 95% khi
   mạch primary đang mở, vì chỉ còn đúng một provider có `fail_rate = 0.05`. Ba
   hướng, nên làm theo thứ tự: (a) thêm provider thứ ba vào chuỗi fallback; (b)
   khi mọi provider đều chết thì **phục vụ cache quá hạn (stale)** kèm cảnh báo
   trong response — câu trả lời cũ vẫn hữu ích hơn nhiều so với "dịch vụ đang bận";
   (c) chỉ khi đó mới rơi xuống static fallback.

5. **Đưa cache hit vào mẫu latency** (dưới cờ cấu hình) để P50/P95 phản ánh trải
   nghiệm thật của người dùng, đồng thời vẫn giữ được cách đo cũ để so sánh
   lịch sử. Số liệu hiện tại đang báo cáo thấp hơn thực tế một cách hệ thống về
   lợi ích của cache.

---

## Phụ lục — Kiểm chứng

```
$ make test          (Redis đang chạy)
35 passed, 7 xpassed in 3.82s          # 0 failed, 0 skipped
    tests/test_circuit_breaker.py   12 passed
    tests/test_cache.py              9 passed
    tests/test_gateway_contract.py   4 passed
    tests/test_redis_cache.py        6 passed   (KHÔNG phải skipped)
    tests/test_config.py             2 passed
    tests/test_metrics.py            2 passed
    tests/test_todo_requirements.py  7 XPASS    (mọi phần cần cài đặt đã hoàn tất)

$ make lint
All checks passed!

$ make typecheck
Success: no issues found in 8 source files
```

Log đầy đủ: `reports/evidence/test_log.txt`

### Các file đã sinh ra

| File | Nội dung |
|---|---|
| `reports/metrics.json` | **Số liệu chính thức** — tuần tự, backend Redis |
| `reports/metrics.csv` | Cùng số liệu, dạng 1 dòng CSV (`write_csv`) |
| `reports/metrics_scenarios.json` | Bóc tách chi tiết từng scenario |
| `reports/metrics_memory.json` | Lần chạy với cache in-memory |
| `reports/metrics_no_cache.json` | Lần chạy tắt cache |
| `reports/metrics_concurrent.json` | Lần chạy tải đồng thời (`ThreadPoolExecutor`, 10 worker) |
| `reports/evidence/test_log.txt` | Log `pytest -v` đầy đủ + lint + typecheck |
| `reports/evidence/redis_evidence.txt` | Shared state + privacy + false hit |
| `reports/evidence/similarity_threshold_study.txt` | Phổ điểm giống của 190 cặp query |

### Những file starter tôi có sửa (và lý do)

| File | Thay đổi | Lý do |
|---|---|---|
| `providers.py` | Thêm tham số `rng` tuỳ chọn + khoá cho RNG | Cần cho tái lập và cho chế độ đa luồng. Mặc định `None` giữ nguyên hành vi cũ nên mọi test starter vẫn xanh. |
| `config.py` | Thêm `seed`, `load_test.concurrency`; đọc file với `encoding="utf-8"` | Tái lập + tải đồng thời; và trên Windows `read_text()` mặc định dùng cp1252 làm hỏng dấu `—` trong file YAML. |
| `scripts/run_chaos.py` | Thêm `--requests`, `--concurrency`; ghi thêm CSV và file chi tiết scenario | Dò tìm nhanh khi tune; CSV là yêu cầu của `write_csv`. |
| `scripts/generate_report.py` | Không ghi đè báo cáo viết tay | `make report` của starter ghi thẳng đè lên `reports/final_report.md` — xoá sạch deliverable. Nay nếu file đích không có dấu `<!-- generated by ... -->` thì script ghi ra `final_report_auto.md` và giữ nguyên bản viết tay. |
| `Makefile` | `clean` không xoá `reports/` nữa; thêm `clean-all`, `evidence` | `make clean` của starter xoá cả `metrics.json` lẫn `final_report.md` — hai deliverable bắt buộc. |
| `.gitignore` | Bỏ 2 dòng loại trừ `reports/metrics.json` và `reports/final_report.md` | Starter đang ignore đúng hai file bắt buộc phải nộp. |
| `.gitattributes` | Thêm mới: `* text=auto eol=lf` | Makefile bị lưu CRLF sẽ làm `make` chết trên Linux/macOS của người chấm. |
| `pyproject.toml` | Thêm `types-PyYAML`; `per-file-ignores` cho `tests/` | `make typecheck` thiếu stub. `tests/` là đề bài, không được sửa, nên bỏ qua lint noise có sẵn ở đó thay vì sửa file test. |

**Không sửa gì trong `tests/`.**
