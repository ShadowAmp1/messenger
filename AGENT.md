# AGENT.md

## Правила работы
- Не трогать секреты и не коммитить `.env` или любые файлы с реальными ключами/токенами.
- Любые изменения в репозитории делать только через Pull Request.
- Перед коммитом обязательно запускать проверки проекта:
  - `python -m ruff check .`
  - `python -m pytest backend/tests`
- Не менять протокол WebSocket (формат событий, поля payload, семантику сообщений) без явного описания изменений в PR.

## Команды запуска (фактически используемые в репозитории)
### Backend
```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend
- Основной путь: фронтенд раздаётся backend-ом по адресам:
  - `http://localhost:8000/`
  - `http://localhost:8000/app`
- Альтернатива для локальной статики (опционально): запустить любой static server в папке `frontend/`.
