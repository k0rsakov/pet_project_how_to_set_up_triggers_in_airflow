# Как настроить Triggers в Airflow

https://www.notion.so/korsak0v/Data-Engineer-185c62fdf79345eb9da9928356884ea0

## О видео


## О проекте


### Виртуальное окружение

Настройка виртуального окружения:

```bash
python3.12 -m venv venv && \
source venv/bin/activate && \
pip install --upgrade pip && \
pip install poetry && \
poetry lock && \
poetry install
```

### Настройка Airflow через Docker

Мы используем Airflow, который собирается при помощи [Dockerfile](Dockerfile)
и [docker-compose.yaml](docker-compose.yaml).

Для запуска контейнера с Airflow, выполните команду:

```bash
docker-compose up -d
```

Веб-сервер Airflow запустится на хосте http://localhost:8080/, если не будет работать данный хост, то необходимо перейти
по хосту http://0.0.0.0:8080/.

#### Добавление пакетов в текущую сборку

Для того чтобы добавить какой-то пакет в текущую сборку, необходимо выполнить следующие шаги:

* Добавить новую строку в [Dockerfile](Dockerfile)
* Выполнить команду:

```bash
docker-compose build
```

* Выполнить команду:

```bash
docker-compose up -d
```
