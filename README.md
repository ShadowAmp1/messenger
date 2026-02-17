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
curl http://localhost:8000/health
curl http://localhost:8000/api/health
curl http://localhost:8000/
```

Ожидается JSON вида:
- `/health` → `{"ok": true, "version": "...", "commit": "..."}`
- `/api/health` → `{"ok": true, "ts": ..., "version": "...", "commit": "..."}`

Ожидается ответ со страницей приложения или JSON-подсказкой, если фронтенд недоступен.

## Логи backend-запросов
Добавлены структурные логи HTTP-запросов (одна JSON-строка на запрос) с полями:
- `method`
- `path`
- `status`
- `latency_ms`
- `user_id` (если токен валиден, иначе `null`)

Пример (формат может отличаться в зависимости от логгера):
```json
{"method": "GET", "path": "/api/chats", "status": 200, "latency_ms": 12.41, "user_id": "alice"}
```


## Политика хранения и удаления медиа
- Медиа-файлы хранятся во внешнем хранилище Cloudinary, в папке `messenger/uploads`.
- В базе хранится ссылка на файл и метаданные сообщения; при удалении сообщения `scope=all` контент скрывается из чата (`deleted_for_all=TRUE`) и больше не выдаётся через API медиа-доступа.
- Прямые Cloudinary-ссылки клиенту не выдаются в сообщениях. Вместо этого API отдаёт временную ссылку `/api/media/access?...` с подписью и TTL (`MEDIA_LINK_TTL_SECONDS`, по умолчанию 300 секунд).
- Временная ссылка проверяется на сервере и работает только для конкретного `chat_id/message_id`, после истечения TTL ссылка становится недействительной.
- Для полноценной hard-delete очистки из Cloudinary можно добавить фоновые задачи удаления по `public_id` (сейчас в проекте используется soft-delete на уровне сообщений).

## Ручной чеклист восстановления после реконнекта
1. Откройте два клиента (например, два браузера) под разными пользователями в одном чате.
2. На клиенте A отключите интернет на 30 секунд (DevTools → Network → Offline).
3. Пока клиент A офлайн, отправьте с клиента B несколько сообщений в этот чат.
4. Включите интернет на клиенте A.
5. Убедитесь, что WebSocket переподключился и клиент A автоматически подтянул пропущенные сообщения (без ручного обновления страницы).
6. Проверьте, что после восстановления всё ещё работают базовые сценарии: логин, список чатов, отправка сообщения, WebSocket-события в реальном времени.

## Ручной чеклист для refresh-cookie
1. Откройте DevTools → Network, выполните вход (`/api/login` или `/api/register`).
2. В ответе проверьте заголовок `Set-Cookie` для `refresh_token` с флагами `HttpOnly`, `Secure`, `SameSite=Lax`, `Path=/api`.
3. Откройте DevTools → Application → Storage → Local Storage и убедитесь, что ключа `refresh_token` нет.
4. Дождитесь истечения access-token (или вручную вызовите `/api/refresh`), убедитесь что запрос уходит без body-токена, а cookie отправляется автоматически.
5. Проверьте базовые сценарии после refresh: список чатов грузится, сообщения отправляются, WebSocket переподключается с новым access-token.
6. Нажмите logout и убедитесь, что `/api/logout` вызывается, а cookie `refresh_token` очищается.


## Тест-пейлоад для проверки XSS/linkify
Отправьте в чат строку:

```text
<b>Alice</b> <img src=x onerror=alert(1)> javascript:alert(1) www.example.com https://example.com?q=<script>
```

Ожидаемое поведение на экране:
- Текст `<b>Alice</b>` и `<img ...>` отображается как обычный текст, без выполнения HTML/JS.
- Имена/статусы участников и сообщения отображаются как текстовые строки (не как HTML-разметка).
- Кликабельными остаются только ссылки с безопасными протоколами `http/https` (например, `www.example.com` и `https://example.com...`).
- `javascript:alert(1)` не должен превращаться в ссылку.
