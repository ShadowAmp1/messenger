# Mini Messenger Android (Google Play readiness)

Этот модуль добавляет нативную Android-обёртку для публикации в Google Play.
Приложение открывает ваш текущий сайт/веб-клиент в `WebView`, поэтому сайт и мобильное приложение используют один и тот же backend, API и аккаунты.

## Что уже подготовлено

- Android-проект на Kotlin (`mobile/android`)
- `applicationId`: `com.minimessenger.app`
- Минимальный SDK: 24 (Android 7.0)
- Target/Compile SDK: 35
- Release-сборка с ProGuard
- Иконка приложения (adaptive icon)

## Настройка URL прод-сайта

В `mobile/android/app/build.gradle.kts` измените:

```kotlin
buildConfigField("String", "BASE_URL", '"https://mini-messenger.onrender.com/"')
```

на ваш production URL (обязательно `https`).

## Сборка AAB для Google Play

1. Откройте `mobile/android` в Android Studio.
2. Создайте signing key:
   - `Build > Generate Signed Bundle / APK > Android App Bundle`
3. Выберите `release` и соберите `.aab`.
4. Загрузите `.aab` в Play Console.

CLI-вариант (если установлен Gradle wrapper):

```bash
./gradlew bundleRelease
```

Файл будет в: `app/build/outputs/bundle/release/app-release.aab`

## Что ещё нужно для публикации

- Политика конфиденциальности (URL)
- Иконка 512x512 PNG и Feature Graphic 1024x500
- Скриншоты телефона (минимум 2)
- Контент-рейтинг
- App access / Data safety формы в Play Console

## Рекомендации перед релизом

- Включить crash analytics (Firebase Crashlytics)
- Добавить deep links (если нужен вход по ссылкам в чат)
- Добавить push-уведомления (FCM)
- Проверить работу загрузки медиа/камеры в WebView
