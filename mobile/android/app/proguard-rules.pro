# Keep WebView JS bridge methods if we add one later.
-keepclassmembers class * {
    @android.webkit.JavascriptInterface <methods>;
}
