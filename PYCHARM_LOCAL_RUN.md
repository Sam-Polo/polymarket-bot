# Запуск бота локально в PyCharm

## 1. Проект и Python

- Открыть папку проекта в PyCharm.
- Нужен **Python 3.10+**.

## 2. Виртуальное окружение

В терминале PyCharm (внизу), из корня проекта:

**Windows (PowerShell):**

```text
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Дальше можно **каждый раз** в том же терминале (после `Activate.ps1`) запускать:

```text
python botv5.py
```

Интерпретатор из проекта в PyCharm (**Settings → Project → Python Interpreter → `.venv`**) нужен **по желанию**: подсветка/проверки под тем же окружением и кнопка **Run** без ручной активации venv.
