# Mini Messenger

Mini Messenger — это небольшой продакшн-ориентированный мессенджер на FastAPI с одностраничным фронтендом (SPA) и WebSocket для realtime-обновлений.

## Стек
- Backend: FastAPI + psycopg (PostgreSQL)
- Frontend: ванильный JS SPA
- Realtime: WebSocket
- Медиа: Cloudinary

## Возможности
- Регистрация и вход по username/password
- JWT + refresh token
- Личные и групповые чаты
- Realtime-доставка сообщений через WebSocket
- Реакции, pin, reply и базовые чат-настройки
- Загрузка медиа (изображения, видео, аудио)
- PWA-режим с service worker

## Локальный запуск

### 1) Backend
```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 2) Frontend
Frontend уже лежит в репозитории и раздаётся backend-ом:
- `http://localhost:8000/`
- `http://localhost:8000/app`

Если хотите открыть статику отдельно, можно использовать любой простой static server в папке `frontend/`.

## Обязательные переменные окружения
Скопируйте `.env.example` в `.env` и заполните значения.

- `DATABASE_URL`
- `JWT_SECRET`
- `CORS_ORIGINS`
- `CLOUDINARY_CLOUD_NAME`
- `CLOUDINARY_API_KEY`
- `CLOUDINARY_API_SECRET`

## Быстрая проверка
```bash
curl http://localhost:8000/api/health
```

Ожидается JSON вида `{"ok": true, "ts": ...}`.
curl http://localhost:8000/
```

Ожидается ответ со страницей приложения или JSON-подсказкой, если фронтенд недоступен.
