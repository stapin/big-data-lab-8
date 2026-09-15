# Big Data Lab 8 — ETL + ML pipeline на Greenplum / Spark / Argo Workflows

Учебный биг-дата пайплайн, который прогоняет данные о продуктах питания (датасет [Open Food Facts](https://world.openfoodfacts.org/)) через полный цикл ETL → ML → загрузка результатов:

1. **Seeder** (`workspace/download_data.py`, PySpark) — загружает сырые данные (`workspace/sample_data.csv`) в Greenplum, таблица `products_raw`.
2. **Datamart Extractor** (`workspace/datamart`, Scala/Spark, `sbt run --mode extract`) — читает `products_raw`, чистит, собирает и масштабирует признаки (`energy-kcal_100g`, `proteins_100g`, `fat_100g`), пишет их в `products_features`.
3. **ML Pipeline** (`workspace/main.py`, PySpark) — читает `products_features`, обучает K-Means (`workspace/model.py`), пишет предсказания в `products_predictions`.
4. **Datamart Loader** (`workspace/datamart`, Scala/Spark, `sbt run --mode load`) — переносит предсказания в финальную витрину `products_clustered`.

Все шаги читают/пишут данные в **Greenplum** (используется образ `projectairws/greenplum`, по факту — обычный PostgreSQL-совместимый протокол/JDBC).

Оркестрация полного пайплайна в Kubernetes выполняется через **Argo Workflows** (`k8s-argo-pipeline.yaml`), мониторинг ресурсов подов — через **VictoriaMetrics + Grafana** (`victoria-metrics.yaml`, `grafana-jobs.yaml`).

Есть также упрощённый локальный запуск через **docker-compose** (без Argo/K8s/мониторинга) — для разработки и ручной прогонки шагов.

---

## Состав репозитория

| Файл | Назначение |
|---|---|
| `docker-compose.yaml` | Локальное окружение: Greenplum + PySpark-контейнер + Scala-datamart-контейнер |
| `Dockerfile` | Образ PySpark-окружения (seeder + ML pipeline) |
| `Dockerfile.scala` | Образ Scala/sbt-окружения (datamart extractor/loader) |
| `k8s-argo-pipeline.yaml` | Namespace, секрет с кредами БД, Greenplum-деплой + Argo `Workflow` со всеми 4 шагами пайплайна |
| `k8s-bigdata-lab.yaml` | Альтернативный вариант без Argo — те же шаги как отдельные `Job` в K8s (последовательность нужно запускать вручную) |
| `victoria-metrics.yaml` | VictoriaMetrics (сбор метрик подов через cAdvisor) в namespace `monitoring` |
| `grafana-jobs.yaml` | Grafana с преднастроенным дашбордом потребления CPU/RAM джобами, datasource — VictoriaMetrics |
| `workspace/` | Код seeder'а, ML-пайплайна и Scala datamart-приложения |

---

## 1. Необходимые данные / переменные окружения

Для подключения к Greenplum используются переменные окружения (см. `workspace/.env` и `workspace/config.py`):

```env
PORT=5432
DB_NAME=gpadmin
USER=gpadmin
PASSWORD=pivotal
```

Для K8s/Argo эти же креды передаются через `Secret` `greenplum-secrets` (namespace `bigdata-lab`):

```yaml
POSTGRES_USER: gpadmin
POSTGRES_PASSWORD: pivotal
POSTGRES_DB: gpadmin
```

Если меняете пароль/пользователя/БД — правьте оба места согласованно (`.env` для локального запуска и `stringData` секрета в `k8s-argo-pipeline.yaml` / `k8s-bigdata-lab.yaml`), иначе Spark/Scala-джобы не смогут законнектиться к БД.

Исходный датасет `openfoodfacts.csv.gz` (~1.2 ГБ) лежит в корне репозитория как справочный источник; сам пайплайн по умолчанию использует уже подготовленную выборку `workspace/sample_data.csv`.

---

## 2. Локальный запуск (docker-compose)

Самый быстрый способ поднять окружение и прогнать шаги руками.

```powershell
docker compose up -d --build
```

Поднимутся:
- `greenplum-container` — БД, порт `5432` проброшен на хост сразу (`localhost:5432`);
- `spark-pure-container` — PySpark-окружение (`./workspace` смонтирован в `/workspace`), сидит в сети `greenplum-db`;
- `scala-datamart-container` — Scala/sbt-окружение для datamart.

Дальше шаги пайплайна запускаются вручную, по порядку:

```powershell
# 1. Засеять сырые данные в Greenplum
docker exec -it spark-pure-container python download_data.py

# 2. Извлечь и подготовить признаки (Scala datamart, extract)
docker exec -it scala-datamart-container sh -c "cd /workspace/datamart && sbt \"runMain DataMart --mode extract\""

# 3. Обучить ML-модель и записать предсказания
docker exec -it spark-pure-container python main.py

# 4. Загрузить финальный результат в витрину (Scala datamart, load)
docker exec -it scala-datamart-container sh -c "cd /workspace/datamart && sbt \"runMain DataMart --mode load\""
```

Остановить окружение: `docker compose down` (добавьте `-v`, если нужно снести volume с данными БД).

---

## 3. Запуск в Kubernetes через Argo Workflows

### 3.1 Предварительные требования

- Работающий кластер Kubernetes (kind/minikube/managed) и `kubectl`, настроенный на него.
- Установленный **Argo Workflows** (контроллер + CLI). В репозитории нет манифестов установки самого Argo — ставится штатно:

```powershell
kubectl create namespace argo
kubectl apply -n argo -f https://github.com/argoproj/argo-workflows/releases/latest/download/install.yaml
```

- Собранные и доступные кластеру Docker-образы, которые использует `k8s-argo-pipeline.yaml`:
  - `pyspark-env-image:v9` (из `Dockerfile`) — seeder + ML pipeline;
  - `scala-datamart-image:v1` (из `Dockerfile.scala`) — extractor/loader.

```powershell
docker build -t pyspark-env-image:v9 -f Dockerfile .
docker build -t scala-datamart-image:v1 -f Dockerfile.scala .

# если кластер локальный (kind) — образы нужно загрузить в него явно
kind load docker-image pyspark-env-image:v9
kind load docker-image scala-datamart-image:v1
# для minikube: minikube image load pyspark-env-image:v9 (и второй образ)
```

Если имена/теги образов меняете — поправьте их в `k8s-argo-pipeline.yaml` (`image:` у шаблонов `seeder-template`, `extractor-template`, `pyspark-model-template`, `loader-template`).

### 3.2 Развёртывание пайплайна

```powershell
kubectl apply -f k8s-argo-pipeline.yaml
```

Это создаст namespace `bigdata-lab`, секрет с кредами БД, деплой Greenplum и сам Argo `Workflow` (граф: `wait-for-db → seed-data → extract-datamart → run-pyspark-model → load-datamart`). Workflow запускается автоматически при апе манифеста (это ресурс `Workflow`, а не `WorkflowTemplate`).

Проверить статус:

```powershell
kubectl get pods -n bigdata-lab -w
argo list -n bigdata-lab
argo logs -n bigdata-lab @latest
```

> Альтернатива без Argo — `k8s-bigdata-lab.yaml`, где те же шаги оформлены как обычные K8s `Job` без графа зависимостей. Там шаги нужно применять/запускать в правильном порядке вручную (create → дождаться Completed → следующий).

### 3.3 Доступ к Argo UI

Сервер Argo UI разворачивается вместе с контроллером в namespace `argo` (сервис `argo-server`). Пробросить порт на хост:

```powershell
kubectl -n argo port-forward svc/argo-server 2746:2746
```

Открыть в браузере: **https://localhost:2746** (self-signed сертификат — браузер попросит подтвердить исключение). По умолчанию у Argo Workflows в этом режиме включена аутентификация `client`, что для локального просмотра обычно можно переключить на `server` (или логиниться токеном ServiceAccount) — для учебного стенда проще всего:

```powershell
kubectl patch deployment argo-server -n argo --type='json' -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":["server","--auth-mode=server"]}]'
```

после чего UI открывается без логина.

---

## 4. Доступ к Grafana

Grafana и VictoriaMetrics разворачиваются в namespace `monitoring`:

```powershell
kubectl apply -f victoria-metrics.yaml
kubectl apply -f grafana-jobs.yaml
```

Пробросить порт Grafana на хост:

```powershell
kubectl -n monitoring port-forward svc/grafana 3000:3000
```

Открыть **http://localhost:3000**. Логин/пароль по умолчанию для образа `grafana/grafana` — **admin / admin** (при первом входе Grafana попросит сменить пароль; том с данными не примонтирован как `PersistentVolume`, поэтому при пересоздании пода настройки/дашборды из UI не переживут рестарт, но провижининг из ConfigMap — datasource VictoriaMetrics и дашборд `Kubernetes Jobs Monitor` — накатится заново автоматически).

Дашборд «Kubernetes Jobs Monitor» уже добавлен через provisioning и показывает пиковые/динамические CPU и RAM по выбранному namespace/поду (удобно смотреть джобы пайплайна: `bigdata-lab` / под нужного шага).

Если нужно посмотреть сырые метрики VictoriaMetrics напрямую:

```powershell
kubectl -n monitoring port-forward svc/victoria-metrics 8428:8428
```

и открыть **http://localhost:8428/vmui**.

---

## 5. Просмотр базы данных (Greenplum)

### Проброс порта

- **docker-compose**: порт БД уже опубликован на хосте — ничего пробрасывать не нужно, подключайтесь на `localhost:5432`.
- **Kubernetes**:

```powershell
kubectl -n bigdata-lab port-forward svc/greenplum-service 5432:5432
```

### Подключение

Креды: `POSTGRES_USER=gpadmin`, `POSTGRES_PASSWORD=pivotal`, `POSTGRES_DB=gpadmin` (см. секцию 1, если меняли).

```powershell
psql "postgresql://gpadmin:pivotal@localhost:5432/gpadmin"
```

Либо любым GUI-клиентом (DBeaver, pgAdmin, DataGrip) с теми же параметрами: host `localhost`, порт `5432`, база `gpadmin`, пользователь `gpadmin`.

Таблицы, которые появляются по мере прохождения пайплайна:

| Таблица | Создаётся на шаге | Содержимое |
|---|---|---|
| `products_raw` | Seeder | Сырые данные из `sample_data.csv` |
| `products_features` | Extractor (Scala) | Отмасштабированные признаки для ML |
| `products_predictions` | ML Pipeline (PySpark) | `id` + номер кластера K-Means |
| `products_clustered` | Loader (Scala) | Финальная витрина (копия предсказаний) |

Готовые SQL-запросы для проверки лежат в `workspace/greenplum_db.session.sql` (`SELECT * FROM products_raw LIMIT 100;`) и `workspace/get_result.sql` (`SELECT * FROM products_clustered LIMIT 100;`).
