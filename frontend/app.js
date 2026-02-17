  const $ = (id) => document.getElementById(id);
  const toggleHidden = (el, hidden) => el.classList.toggle("is-hidden", !!hidden);
  const escapeHtml = (v) => String(v || "").replace(/[&<>"']/g, (ch)=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[ch]));
  let lastSidebarFocus = null;

  let token = localStorage.getItem("token") || "";
  // Cleanup legacy client-side refresh token storage.
  localStorage.removeItem("refresh_token");
  let me = localStorage.getItem("username") || "";
  let avatarUrl = localStorage.getItem("avatar_url") || "";
  let replyTo = null;
  let displayName = localStorage.getItem("display_name") || "";
  let profileBio = localStorage.getItem("profile_bio") || "";
  let stories = [];
  let avatarHistory = [];
  let contacts = [];
  const HELP_ONBOARDING_KEY = "helpOnboardingShown";

  const REACTION_EMOJIS = ["👍","❤️","😂","😮","🔥","🎉","👏","🤝","🙏","😢","😡","💯"];

  const FEATURE_FLAGS = {
    calls: {
      enabled: true,
      unstable: true,
      hideWhenUnstable: false
    }
  };


  const THEME_KEY = "theme";
  function applyTheme(theme){
    const root = document.documentElement;
    const nextTheme = theme === "dark" ? "dark" : "light";
    root.setAttribute("data-theme", nextTheme);
    localStorage.setItem(THEME_KEY, nextTheme);
    const btn = $("btnThemeToggle");
    if (btn) btn.textContent = nextTheme === "dark" ? "☀️ Тема" : "🌙 Тема";
  }
  function initTheme(){
    const saved = localStorage.getItem(THEME_KEY);
    if (saved === "dark" || saved === "light") return applyTheme(saved);
    const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    applyTheme(prefersDark ? "dark" : "light");
  }
  function toggleTheme(){
    const current = document.documentElement.getAttribute("data-theme") || "light";
    applyTheme(current === "dark" ? "light" : "dark");
  }


  let deferredInstallPrompt = null;

  function initMobileAppBridge(){
    const installBtn = $("btnInstallApp");
    if (!installBtn) return;

    const inStandalone = window.matchMedia?.("(display-mode: standalone)")?.matches || window.navigator.standalone;
    if (inStandalone){
      installBtn.textContent = "✅ Приложение";
      installBtn.disabled = true;
      installBtn.title = "Приложение уже запущено";
      return;
    }

    installBtn.addEventListener("click", async ()=>{
      if (deferredInstallPrompt){
        deferredInstallPrompt.prompt();
        try { await deferredInstallPrompt.userChoice; } catch(_){}
        deferredInstallPrompt = null;
        installBtn.textContent = "📱 Приложение";
        return;
      }
      addSystem("Откройте меню браузера и выберите «Установить приложение». После установки сайт и приложение используют один и тот же аккаунт.");
    });

    window.addEventListener("beforeinstallprompt", (event)=>{
      event.preventDefault();
      deferredInstallPrompt = event;
      installBtn.textContent = "⬇️ Установить";
    });

    window.addEventListener("appinstalled", ()=>{
      deferredInstallPrompt = null;
      installBtn.textContent = "✅ Установлено";
      installBtn.disabled = true;
      addSystem("Мобильное приложение установлено и связано с сайтом.");
    });
  }

  function registerServiceWorker(){
    if (!("serviceWorker" in navigator)) return;
    window.addEventListener("load", ()=>{
      navigator.serviceWorker.register("/sw.js").catch(()=>{});
    });
  }

  // GLOBAL WS
  let ws = null;

  let chats = [];
  let activeChatFilter = "all";
  let sidebarMoreMenuOpen = false;
  let activeChatId = localStorage.getItem("activeChatId") || "";
  let activeChatTitle = "";
  let activeChatType = "";
  let activeChatCreatedBy = "";

  const msgElById = new Map();
  let lastMsgId = 0;
  let oldestLoadedMessageId = null;
  let hasMoreHistory = true;
  let isHistoryLoading = false;

  function getLastMessageStorageKey(){
    return me ? `lastMessageId:${me}` : "lastMessageId";
  }

  function loadLastMessageId(){
    const raw = localStorage.getItem(getLastMessageStorageKey()) || "0";
    const parsed = Number(raw);
    return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0;
  }

  function persistLastMessageId(nextId){
    const current = loadLastMessageId();
    const safeId = Number(nextId || 0);
    if (!Number.isFinite(safeId) || safeId <= current) return;
    localStorage.setItem(getLastMessageStorageKey(), String(Math.floor(safeId)));
  }

  function isMobile(){ return window.matchMedia("(max-width: 900px)").matches; }

  const modalManager = (() => {
    const registry = new Map();
    const stack = [];

    const focusableSelector = [
      "button:not([disabled])",
      "[href]",
      "input:not([disabled])",
      "select:not([disabled])",
      "textarea:not([disabled])",
      "[tabindex]:not([tabindex='-1'])"
    ].join(",");

    function getFocusable(panel){
      return Array.from(panel.querySelectorAll(focusableSelector)).filter((el)=> !el.hidden && el.offsetParent !== null);
    }

    function getTop(){
      const topId = stack[stack.length - 1];
      if (topId && registry.has(topId)) return registry.get(topId);
      const opened = Array.from(registry.values()).filter((modal)=> modal.overlay.classList.contains("open"));
      return opened.length ? opened[opened.length - 1] : null;
    }

    function register({ overlayId, panelSelector, close, ariaLabel }){
      const overlay = $(overlayId);
      if (!overlay) return;
      const panel = overlay.querySelector(panelSelector);
      if (!panel) return;

      panel.setAttribute("role", "dialog");
      panel.setAttribute("aria-modal", "true");
      if (ariaLabel) panel.setAttribute("aria-label", ariaLabel);

      const modal = {
        id: overlayId,
        overlay,
        panel,
        close,
        lastTrigger: null
      };

      overlay.addEventListener("click", (e)=>{
        if (e.target === overlay) close();
      });

      registry.set(overlayId, modal);
    }

    function open(overlayId){
      const modal = registry.get(overlayId);
      if (!modal) return;
      modal.lastTrigger = document.activeElement;
      modal.overlay.classList.add("open");
      modal.overlay.setAttribute("aria-hidden", "false");
      const idx = stack.indexOf(overlayId);
      if (idx !== -1) stack.splice(idx, 1);
      stack.push(overlayId);
      const focusable = getFocusable(modal.panel);
      const target = focusable[0] || modal.panel;
      if (target === modal.panel && !target.hasAttribute("tabindex")) target.setAttribute("tabindex", "-1");
      setTimeout(()=> target.focus(), 0);
    }

    function close(overlayId, { restoreFocus=true } = {}){
      const modal = registry.get(overlayId);
      if (!modal) return;
      modal.overlay.classList.remove("open");
      modal.overlay.setAttribute("aria-hidden", "true");
      const idx = stack.indexOf(overlayId);
      if (idx !== -1) stack.splice(idx, 1);
      if (restoreFocus && modal.lastTrigger && typeof modal.lastTrigger.focus === "function" && document.contains(modal.lastTrigger)){
        modal.lastTrigger.focus();
      }
    }

    function closeTop(){
      const top = getTop();
      if (!top || typeof top.close !== "function") return false;
      top.close();
      return true;
    }

    document.addEventListener("keydown", (e)=>{
      const top = getTop();
      if (!top) return;
      if (e.key === "Escape"){
        e.preventDefault();
        e.stopPropagation();
        top.close();
        return;
      }
      if (e.key !== "Tab") return;
      const focusable = getFocusable(top.panel);
      if (!focusable.length) return;
      e.preventDefault();
      const current = document.activeElement;
      const idx = focusable.indexOf(current);
      if (idx === -1){
        (e.shiftKey ? focusable[focusable.length - 1] : focusable[0]).focus();
        return;
      }
      const next = e.shiftKey
        ? (idx === 0 ? focusable[focusable.length - 1] : focusable[idx - 1])
        : (idx === focusable.length - 1 ? focusable[0] : focusable[idx + 1]);
      next.focus();
    });

    return { register, open, close, closeTop };
  })();

  // --- sound notify ---
  let audioCtx = null;
  function beep(){
    try{
      if (!audioCtx) audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      const ctx = audioCtx;
      const o = ctx.createOscillator();
      const g = ctx.createGain();
      o.type = "sine"; o.frequency.value = 880;
      g.gain.value = 0.0001;
      o.connect(g); g.connect(ctx.destination);
      const t = ctx.currentTime;
      g.gain.setValueAtTime(0.0001, t);
      g.gain.exponentialRampToValueAtTime(0.08, t + 0.02);
      g.gain.exponentialRampToValueAtTime(0.0001, t + 0.18);
      o.start(t); o.stop(t + 0.2);
    }catch(_){}
  }


  function requestNotificationPermissionIfNeeded(){
    if ("Notification" in window && Notification.permission === "default"){
      Notification.requestPermission().catch(()=>{});
    }
  }

  // --- Drawer ---
  const sidebar = $("sidebar");
  const backdrop = $("drawerBackdrop");
  const sidebarToggleBtn = $("btnToggleSidebar");
  function openSidebar(){
    lastSidebarFocus = document.activeElement;
    sidebar.classList.add("open");
    sidebar.setAttribute("aria-hidden", "false");
    backdrop.classList.add("open");
    backdrop.setAttribute("aria-hidden", "false");
    sidebarToggleBtn.setAttribute("aria-expanded", "true");
    document.body.classList.add("drawer-open");
    const firstBtn = sidebar.querySelector("button, [href], input, select, textarea, [tabindex]:not([tabindex='-1'])");
    firstBtn?.focus();
  }
  function closeSidebar({ restoreFocus=true } = {}){
    sidebar.classList.remove("open");
    sidebar.setAttribute("aria-hidden", "true");
    backdrop.classList.remove("open");
    backdrop.setAttribute("aria-hidden", "true");
    sidebarToggleBtn.setAttribute("aria-expanded", "false");
    document.body.classList.remove("drawer-open");
    if (restoreFocus && lastSidebarFocus && typeof lastSidebarFocus.focus === "function"){
      lastSidebarFocus.focus();
    }
  }
  function toggleSidebar(){ sidebar.classList.contains("open") ? closeSidebar() : openSidebar(); }

  function openSidebarMoreMenu(){
    const menu = $("sidebarMoreMenu");
    const trigger = $("btnSidebarMore");
    if (!menu || !trigger) return;
    menu.classList.add("open");
    menu.setAttribute("aria-hidden", "false");
    trigger.setAttribute("aria-expanded", "true");
    sidebarMoreMenuOpen = true;
  }

  function closeSidebarMoreMenu(){
    const menu = $("sidebarMoreMenu");
    const trigger = $("btnSidebarMore");
    if (!menu || !trigger) return;
    menu.classList.remove("open");
    menu.setAttribute("aria-hidden", "true");
    trigger.setAttribute("aria-expanded", "false");
    sidebarMoreMenuOpen = false;
  }

  function toggleSidebarMoreMenu(){
    sidebarMoreMenuOpen ? closeSidebarMoreMenu() : openSidebarMoreMenu();
  }
  function syncSidebarTopOffset(){
    if (!isMobile()){
      document.documentElement.style.removeProperty("--sidebar-top");
      return;
    }
    const topEl = document.querySelector(".top");
    const cardEl = document.querySelector(".card");
    if (!topEl || !cardEl) return;
    const topRect = topEl.getBoundingClientRect();
    const cardRect = cardEl.getBoundingClientRect();
    const offset = Math.max(0, Math.round(topRect.bottom - cardRect.top));
    document.documentElement.style.setProperty("--sidebar-top", `${offset}px`);
  }

  // --- Scroll + read ---
  const toBottomBtn = $("btnToBottom");
  function isNearBottom(el, px=160){
    return (el.scrollHeight - el.scrollTop - el.clientHeight) < px;
  }
  function scrollToBottom(el){ el.scrollTop = el.scrollHeight; }
  function updateToBottom(){
    const box = $("msgs");
    toBottomBtn.classList.toggle("show", !isNearBottom(box));
    if (isNearBottom(box)) maybeMarkRead();
  }

  function setStatus(s){ $("status").textContent = s || "—"; }
  function setNet(s){ $("net").textContent = s || "не в сети"; }

  let offlineMode = false;
  function setOfflineMode(isOffline){
    offlineMode = !!isOffline;
    const banner = $("offlineBanner");
    if (banner) toggleHidden(banner, !offlineMode);
    if (offlineMode) setNet("offline");
    else if (!ws || ws.readyState !== 1) setNet("connecting…");
  }

  function addSystem(text){
    showSystemToast(text);
  }

  function showNetworkError(message){
    showSystemToast(`⚠️ ${String(message || "Сетевая ошибка")}`, 7000, "error");
  }

  function showSystemToast(text, ttlMs = 0, variant = "default"){
    const host = $("systemToasts");
    if (!host) return;
    const toast = document.createElement("div");
    toast.className = "system-toast" + (variant === "error" ? " error" : "");
    toast.textContent = String(text || "").trim() || "System";
    host.appendChild(toast);

    const life = ttlMs > 0 ? ttlMs : (5000 + Math.floor(Math.random() * 3001));
    const fadeAt = Math.max(0, life - 280);
    window.setTimeout(() => toast.classList.add("hide"), fadeAt);
    window.setTimeout(() => toast.remove(), life + 120);
  }

  function isSystemSender(message){
    const sender = String(message?.sender || message?.username || "").trim().toLowerCase();
    return sender === "system";
  }

  function fmtTs(ts){
    try{
      const d = new Date((ts||0)*1000);
      return d.toLocaleString(undefined, {hour:"2-digit", minute:"2-digit"});
    }catch{ return "—"; }
  }

  function extractSafeHttpUrl(value){
    const raw = String(value || "").trim();
    if (!raw) return "";
    const normalized = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    try{
      const parsed = new URL(normalized);
      const protocol = parsed.protocol.toLowerCase();
      if (protocol !== "http:" && protocol !== "https:") return "";
      return parsed.toString();
    }catch{
      return "";
    }
  }

  function inferMediaKind(url, fallback=""){
    const kind = String(fallback || "").toLowerCase();
    if (kind === "image" || kind === "video" || kind === "audio") return kind;
    const clean = String(url || "").split("?")[0].toLowerCase();
    if (/(\.jpg|\.jpeg|\.png|\.gif|\.webp|\.bmp|\.svg)$/.test(clean)) return "image";
    if (/(\.mp4|\.webm|\.mov|\.m4v)$/.test(clean)) return "video";
    if (/(\.mp3|\.ogg|\.wav|\.m4a|\.aac|\.webm)$/.test(clean)) return "audio";
    return "file";
  }

  function openMediaViewer(url, kind="", title="Медиа"){
    if (!url) return;
    const mediaKind = inferMediaKind(url, kind);
    const holder = $("mediaViewerContent");
    holder.innerHTML = "";
    if (mediaKind === "video"){
      const video = document.createElement("video");
      video.src = url;
      video.controls = true;
      video.autoplay = true;
      video.playsInline = true;
      holder.appendChild(video);
    } else {
      const img = document.createElement("img");
      img.src = url;
      img.alt = title || "media";
      holder.appendChild(img);
    }
    $("mediaViewerTitle").textContent = title || "Медиа";
    modalManager.open("mediaViewerOverlay");
  }

  function closeMediaViewer(){
    modalManager.close("mediaViewerOverlay");
    $("mediaViewerContent").innerHTML = "";
  }

  const callUi = {
    overlay: $("callOverlay"),
    title: $("callTitle"),
    hint: $("callHint"),
    localVideo: $("callLocalVideo"),
    micBtn: $("btnToggleCallMic"),
    camBtn: $("btnToggleCallCamera")
  };
  let callState = {
    active:false,
    mode:"voice",
    stream:null,
    micOn:true,
    camOn:false,
    callId:"",
    chatId:"",
    status:"idle",
    startedAt:0,
    connectedAt:0
  };
  let callRingTimer = null;
  let callTickerTimer = null;
  const pendingIncomingCalls = new Map();

  function resetCallState(){
    callState = {
      active:false,
      mode:"voice",
      stream:null,
      micOn:true,
      camOn:false,
      callId:"",
      chatId:"",
      status:"idle",
      startedAt:0,
      connectedAt:0
    };
  }

  function stopCallTimers(){
    if (callRingTimer){ clearTimeout(callRingTimer); callRingTimer = null; }
    if (callTickerTimer){ clearInterval(callTickerTimer); callTickerTimer = null; }
  }

  function startCallTicker(){
    stopCallTimers();
    callTickerTimer = setInterval(()=>{
      if (!callState.active || callState.status !== "connected") return;
      const sec = Math.max(0, Math.floor(Date.now()/1000) - Number(callState.connectedAt || Math.floor(Date.now()/1000)));
      callUi.hint.textContent = `Разговор: ${formatCallDuration(sec)}`;
    }, 1000);
  }

  function wsSendCall(type, extra={}){
    if (!ws || ws.readyState !== 1) return;
    try{
      ws.send(JSON.stringify({
        type,
        chat_id: String(extra.chat_id || callState.chatId || activeChatId || ""),
        call_id: String(extra.call_id || callState.callId || ""),
        mode: String(extra.mode || callState.mode || "voice"),
        started_at: Number(extra.started_at || callState.startedAt || Math.floor(Date.now()/1000)),
        duration: Number(extra.duration || 0),
        reason: String(extra.reason || "")
      }));
    }catch(_){ }
  }

  let chatActionsMenuOpen = false;

  function closeChatActionsMenu(){
    const menu = $("chatActionsMenu");
    const toggle = $("btnChatActions");
    if (!menu || !toggle) return;
    chatActionsMenuOpen = false;
    menu.classList.remove("open");
    menu.setAttribute("aria-hidden", "true");
    toggle.setAttribute("aria-expanded", "false");
  }

  function toggleChatActionsMenu(){
    const menu = $("chatActionsMenu");
    const toggle = $("btnChatActions");
    if (!menu || !toggle) return;
    chatActionsMenuOpen = !chatActionsMenuOpen;
    menu.classList.toggle("open", chatActionsMenuOpen);
    menu.setAttribute("aria-hidden", chatActionsMenuOpen ? "false" : "true");
    toggle.setAttribute("aria-expanded", chatActionsMenuOpen ? "true" : "false");
  }

  function updateChatActionState(){
    const disabled = !activeChatId;
    const toggle = $("btnChatActions");
    if (toggle) toggle.disabled = disabled;

    const callsAreBeta = FEATURE_FLAGS.calls.unstable;
    const callsFeatureAvailable = FEATURE_FLAGS.calls.enabled && !(callsAreBeta && FEATURE_FLAGS.calls.hideWhenUnstable);

    ["btnChatActionInfo", "btnChatActionMedia", "btnChatActionMembers"].forEach((id)=>{
      const btn = $(id);
      if (btn) btn.disabled = disabled;
    });

    ["btnChatActionVoiceCall", "btnChatActionVideoCall"].forEach((id)=>{
      const btn = $(id);
      if (!btn) return;
      btn.disabled = disabled || !callsFeatureAvailable;
      btn.classList.toggle("is-hidden", !callsFeatureAvailable);
      btn.setAttribute("aria-hidden", callsFeatureAvailable ? "false" : "true");
      const betaBadge = btn.querySelector(".beta-badge");
      if (betaBadge) betaBadge.classList.toggle("is-hidden", !callsAreBeta);
    });

    if (disabled) closeChatActionsMenu();
  }

  function updateCallUi(){
    if (!callState.active) return;
    callUi.micBtn.textContent = callState.micOn ? "🎙 Микрофон" : "🔇 Микрофон";
    callUi.camBtn.textContent = callState.camOn ? "📷 Камера" : "🚫 Камера";
    callUi.camBtn.disabled = false;
  }

  async function startCall(mode="voice"){
    if (!token) return openAuth("login");
    if (!activeChatId) return addSystem("⚠️ Сначала выбери чат.");
    const isVideo = mode === "video";
    try{
      if (callState.active) endCall({ silent:true, emit:false });
      const stream = await navigator.mediaDevices.getUserMedia({ audio:true, video:isVideo });
      const callId = `${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
      const startedAt = Math.floor(Date.now()/1000);
      callState = { active:true, mode, stream, micOn:true, camOn:isVideo, callId, chatId:activeChatId, status:"dialing", startedAt, connectedAt:0 };

      callUi.title.textContent = isVideo ? `Видеозвонок • ${activeChatTitle}` : `Голосовой звонок • ${activeChatTitle}`;
      callUi.hint.textContent = "Ожидаем ответа собеседника…";
      if (isVideo){
        callUi.localVideo.classList.remove("is-hidden");
        callUi.localVideo.srcObject = stream;
      } else {
        callUi.localVideo.classList.add("is-hidden");
        callUi.localVideo.srcObject = null;
      }
      modalManager.open("callOverlay");
      setStatus(isVideo ? "🎥 Видеозвонок" : "📞 Голосовой звонок");
      updateCallUi();

      wsSendCall("call_offer", { chat_id: activeChatId, call_id: callId, mode, started_at: startedAt });
      callRingTimer = setTimeout(async ()=>{
        if (!callState.active || callState.callId !== callId || callState.status !== "dialing") return;
        wsSendCall("call_timeout", { chat_id: activeChatId, call_id: callId, mode, started_at: startedAt });
        await pushCallLog({ kind:mode, status:"missed", started_at:startedAt, duration:0 }, activeChatId);
        endCall({ silent:true, emit:false });
        addSystem("☎️ Пропущенный звонок.");
      }, 30000);
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  }

  async function handleIncomingOffer(data){
    const chatId = String(data.chat_id || "");
    const callId = String(data.call_id || "");
    const mode = data.mode === "video" ? "video" : "voice";
    const startedAt = Number(data.started_at || Math.floor(Date.now()/1000));
    if (!chatId || !callId) return;

    pendingIncomingCalls.set(callId, { chatId, mode, startedAt, from: data.username || "" });
    const who = data.username || "собеседник";
    addSystem(`${mode === "video" ? "🎥" : "📞"} Входящий звонок от ${who}.`);

    if (chatId === activeChatId && !callState.active){
      const accepted = confirm(`${mode === "video" ? "Видеозвонок" : "Голосовой звонок"} от ${who}. Ответить?`);
      if (!accepted){
        wsSendCall("call_reject", { chat_id: chatId, call_id: callId, mode, started_at: startedAt, reason:"declined" });
        pendingIncomingCalls.delete(callId);
        await pushCallLog({ kind:mode, status:"rejected", started_at:startedAt, duration:0 }, chatId);
        return;
      }
      try{
        const stream = await navigator.mediaDevices.getUserMedia({ audio:true, video:mode === "video" });
        callState = {
          active:true,
          mode,
          stream,
          micOn:true,
          camOn:mode === "video",
          callId,
          chatId,
          status:"connected",
          startedAt,
          connectedAt:Math.floor(Date.now()/1000)
        };
        callUi.title.textContent = mode === "video" ? `Видеозвонок • ${who}` : `Голосовой звонок • ${who}`;
        callUi.hint.textContent = "Разговор: 0:00";
        if (mode === "video"){
          callUi.localVideo.classList.remove("is-hidden");
          callUi.localVideo.srcObject = stream;
        } else {
          callUi.localVideo.classList.add("is-hidden");
          callUi.localVideo.srcObject = null;
        }
        modalManager.open("callOverlay");
        updateCallUi();
        startCallTicker();
        wsSendCall("call_answer", { chat_id: chatId, call_id: callId, mode, started_at: startedAt });
        pendingIncomingCalls.delete(callId);
      }catch(e){
        wsSendCall("call_reject", { chat_id: chatId, call_id: callId, mode, started_at: startedAt, reason:"media_error" });
        pendingIncomingCalls.delete(callId);
        addSystem("⚠️ Не удалось принять звонок: " + (e.message || e));
      }
    }
  }

  async function toggleCallCamera(){
    if (!callState.active || !callState.stream) return;
    if (callState.camOn){
      callState.stream.getVideoTracks().forEach((track)=>{ track.enabled = false; track.stop(); callState.stream.removeTrack(track); });
      callState.camOn = false;
      callUi.localVideo.srcObject = null;
      callUi.localVideo.classList.add("is-hidden");
      updateCallUi();
      return;
    }
    try{
      const camera = await navigator.mediaDevices.getUserMedia({ video:true });
      const [track] = camera.getVideoTracks();
      if (!track) return;
      callState.stream.addTrack(track);
      callState.camOn = true;
      callUi.localVideo.classList.remove("is-hidden");
      callUi.localVideo.srcObject = new MediaStream([track]);
      updateCallUi();
    }catch(e){
      addSystem("⚠️ Камера недоступна: " + (e.message || e));
    }
  }

  function toggleCallMic(){
    if (!callState.active || !callState.stream) return;
    callState.micOn = !callState.micOn;
    callState.stream.getAudioTracks().forEach((track)=> track.enabled = callState.micOn);
    updateCallUi();
  }

  async function endCall({ silent=false, emit=true } = {}){
    if (!callState.active) return;
    const ended = { ...callState };
    stopCallTimers();
    try{ callState.stream?.getTracks().forEach((track)=> track.stop()); }catch(_){ }
    callUi.localVideo.srcObject = null;
    callUi.localVideo.classList.add("is-hidden");
    modalManager.close("callOverlay");

    if (emit && ended.callId && ended.chatId){
      const duration = ended.status === "connected" ? Math.max(0, Math.floor(Date.now()/1000) - Number(ended.connectedAt || Math.floor(Date.now()/1000))) : 0;
      wsSendCall("call_end", { chat_id: ended.chatId, call_id: ended.callId, mode: ended.mode, started_at: ended.startedAt, duration });
      await pushCallLog({ kind:ended.mode, status:"ended", started_at:ended.startedAt || Math.floor(Date.now()/1000), duration }, ended.chatId);
    }

    if (!silent) addSystem("☎️ Звонок завершён.");
    resetCallState();
    setStatus(activeChatTitle ? `online • ${activeChatTitle}` : "—");
  }

  function formatCallDuration(seconds){
    const total = Math.max(0, Number(seconds) || 0);
    const mins = Math.floor(total / 60);
    const secs = total % 60;
    return `${mins}:${String(secs).padStart(2, "0")}`;
  }

  function renderCallLog(node, msg){
    if (!node || !msg) return false;

    const raw = String(msg.text || "").trim();
    if (!raw.startsWith("__call_log__:")) return false;

    let payload = null;
    try{
      payload = JSON.parse(raw.slice("__call_log__:".length));
    }catch(_){
      return false;
    }

    const kind = payload.kind === "video" ? "🎥 Видеозвонок" : "📞 Голосовой звонок";
    const statusMap = {
      ended: "завершён",
      missed: "пропущен",
      rejected: "отклонён"
    };
    const statusText = statusMap[payload.status] || "завершён";
    const duration = formatCallDuration(payload.duration);

    node.className = "call-log";
    node.textContent = `${kind} • ${statusText} • ${duration}`;
    return true;
  }

  async function pushCallLog(payload, chatId){
    if (!token || !chatId) return;
    const kind = payload?.kind === "video" ? "video" : "voice";
    const status = String(payload?.status || "ended");
    const duration = Math.max(0, Math.floor(Number(payload?.duration || 0)));
    const startedAt = Math.floor(Number(payload?.started_at || Math.floor(Date.now()/1000)));

    try{
      await api("/api/messages", "POST", {
        chat_id: chatId,
        text: `__call_log__:${JSON.stringify({ kind, status, duration, started_at: startedAt })}`
      });
    }catch(_){
      // keep call flow resilient even when message logging fails
    }
  }

  function createMessageAvatar(sender, isMine, senderAvatarUrl){
    const initial = String(sender || "?").trim().charAt(0).toUpperCase() || "?";
    const preferredAvatar = (isMine ? avatarUrl : senderAvatarUrl) || senderAvatarUrl;
    if (preferredAvatar){
      const img = document.createElement("img");
      img.className = "msg-avatar";
      img.src = preferredAvatar;
      img.alt = sender || "me";
      img.loading = "lazy";
      return img;
    }
    const badge = document.createElement("div");
    badge.className = "msg-avatar";
    badge.textContent = initial;
    badge.title = sender || "user";
    return badge;
  }

  async function request(path, { method = "GET", body = null, headers = {}, auth = true, retry = true } = {}){
    if (!navigator.onLine){
      setOfflineMode(true);
      throw new Error("Нет подключения к интернету.");
    }

    const reqHeaders = { ...(headers || {}) };
    const opts = { method, headers: reqHeaders, credentials: "same-origin" };
    if (auth && token) reqHeaders["Authorization"] = `Bearer ${token}`;

    if (body instanceof FormData){
      opts.body = body;
    } else if (body !== null && body !== undefined){
      reqHeaders["Content-Type"] = "application/json";
      opts.body = JSON.stringify(body);
    }

    let res;
    try{
      res = await fetch(path, opts);
      setOfflineMode(false);
    }catch(_){
      setOfflineMode(true);
      throw new Error("Сервер недоступен. Проверьте интернет и повторите попытку.");
    }

    const raw = await res.text();
    let data = {};
    try { data = raw ? JSON.parse(raw) : {}; } catch { data = { detail: raw }; }

    if (res.status === 401 && retry && auth && await tryRefresh()){
      return request(path, { method, body, headers, auth, retry: false });
    }

    if (!res.ok){
      throw new Error((data && data.detail) ? String(data.detail) : `${res.status} ${res.statusText}`);
    }
    return data;
  }

  async function tryRefresh(){
    try{
      const data = await request("/api/refresh", {
        method: "POST",
        auth: false,
        retry: false
      });
      token = data.token || "";
      localStorage.setItem("token", token);
      return !!token;
    }catch(_){
      return false;
    }
  }

  async function api(path, method="GET", body=null){
    return request(path, { method, body, auth: true });
  }

  // =========================
  // Voice waveform player
  // =========================
  const VOICE = { peaksCache: new Map(), audioCache: new Map(), players: new Set() };
  function clamp(v,a,b){ return Math.max(a, Math.min(b, v)); }
  function fmtClock(sec){
    sec = Math.max(0, sec || 0);
    const m = String(Math.floor(sec/60)).padStart(1,"0");
    const s = String(Math.floor(sec%60)).padStart(2,"0");
    return `${m}:${s}`;
  }
  async function getPeaks(url, samples = 120){
    if (VOICE.peaksCache.has(url)) return VOICE.peaksCache.get(url);
    const res = await fetch(url, { mode: "cors" });
    if (!res.ok) throw new Error(`waveform fetch failed: ${res.status}`);
    const buf = await res.arrayBuffer();
    const Ctx = window.AudioContext || window.webkitAudioContext;
    if (!Ctx) throw new Error("AudioContext not supported");
    const ctx = new Ctx();
    const audioBuf = await ctx.decodeAudioData(buf.slice(0));
    try{ await ctx.close(); }catch(_){}
    const ch = audioBuf.getChannelData(0);
    const block = Math.floor(ch.length / samples) || 1;
    const peaks = new Float32Array(samples);
    for (let i=0;i<samples;i++){
      let start = i * block;
      let end = Math.min(ch.length, start + block);
      let max = 0;
      for (let j=start;j<end;j++){
        const v = Math.abs(ch[j]);
        if (v > max) max = v;
      }
      peaks[i] = max;
    }
    let pmax = 0;
    for (let i=0;i<peaks.length;i++) pmax = Math.max(pmax, peaks[i]);
    const k = pmax > 0 ? (1 / pmax) : 1;
    for (let i=0;i<peaks.length;i++) peaks[i] *= k;
    VOICE.peaksCache.set(url, peaks);
    return peaks;
  }
  function getOrCreateAudio(url){
    if (VOICE.audioCache.has(url)) return VOICE.audioCache.get(url);
    const a = new Audio(url);
    a.preload = "metadata";
    a.crossOrigin = "anonymous";
    VOICE.audioCache.set(url, a);
    return a;
  }
  function stopAllExcept(except){
    for (const p of VOICE.players){
      if (p !== except) p.stop();
    }
  }
  function drawWave(canvas, peaks, progress01){
    const dpr = window.devicePixelRatio || 1;
    const cssW = canvas.clientWidth || 300;
    const cssH = canvas.clientHeight || 34;
    const W = Math.floor(cssW * dpr);
    const H = Math.floor(cssH * dpr);
    if (canvas.width !== W) canvas.width = W;
    if (canvas.height !== H) canvas.height = H;
    const ctx = canvas.getContext("2d");
    ctx.clearRect(0,0,W,H);
    const n = peaks.length;
    const gap = Math.floor(2 * dpr);
    const barW = Math.max(1, Math.floor((W - gap*(n-1)) / n));
    const mid = Math.floor(H/2);
    const maxBar = Math.floor(H * 0.78);
    const progX = Math.floor(W * clamp(progress01, 0, 1));
    for (let i=0;i<n;i++){
      const x = i * (barW + gap);
      const amp = peaks[i];
      const h = Math.max(2*dpr, Math.floor(amp * maxBar));
      const y = mid - Math.floor(h/2);
      const filled = (x + barW) <= progX;
      ctx.fillStyle = filled ? "rgba(230,238,252,.95)" : "rgba(230,238,252,.35)";
      ctx.fillRect(x, y, barW, h);
    }
  }
  function createVoicePlayer(url){
    const root = document.createElement("div");
    root.className = "voice";
    const btn = document.createElement("div");
    btn.className = "play";
    btn.textContent = "▶";
    const waveWrap = document.createElement("div");
    waveWrap.className = "wave";
    const canvas = document.createElement("canvas");
    canvas.height = 34;
    const timeRow = document.createElement("div");
    timeRow.className = "time";
    const left = document.createElement("span");
    left.textContent = "0:00";
    const right = document.createElement("span");
    right.textContent = "0:00";
    timeRow.appendChild(left);
    timeRow.appendChild(right);
    waveWrap.appendChild(canvas);
    waveWrap.appendChild(timeRow);
    root.appendChild(btn);
    root.appendChild(waveWrap);

    const audio = getOrCreateAudio(url);

    const player = {
      root, btn, canvas, audio,
      peaks: null,
      raf: 0,
      destroyed: false,
      async init(){
        try{
          this.peaks = await getPeaks(url, 120);
          if (this.destroyed) return;
          drawWave(this.canvas, this.peaks, 0);
        }catch(_){
          this.peaks = new Float32Array(120);
          for (let i=0;i<this.peaks.length;i++) this.peaks[i] = 0.15;
          drawWave(this.canvas, this.peaks, 0);
        }
      },
      setBtn(){ this.btn.textContent = this.audio.paused ? "▶" : "⏸"; },
      tick(){
        if (this.destroyed) return;
        const dur = this.audio.duration || 0;
        const cur = this.audio.currentTime || 0;
        left.textContent = fmtClock(cur);
        right.textContent = dur ? fmtClock(dur) : "0:00";
        const p = dur ? (cur / dur) : 0;
        if (this.peaks) drawWave(this.canvas, this.peaks, p);
        this.raf = requestAnimationFrame(()=> this.tick());
      },
      playPause(){
        stopAllExcept(this);
        if (this.audio.paused) this.audio.play().catch(()=>{});
        else this.audio.pause();
      },
      stop(){
        try{ this.audio.pause(); }catch(_){}
        try{ this.audio.currentTime = 0; }catch(_){}
        this.setBtn();
        if (this.peaks) drawWave(this.canvas, this.peaks, 0);
        if (this.raf) cancelAnimationFrame(this.raf);
        this.raf = 0;
      },
      seekByClientX(clientX){
        const r = this.canvas.getBoundingClientRect();
        const x = clamp(clientX - r.left, 0, r.width);
        const p = r.width ? (x / r.width) : 0;
        const dur = this.audio.duration || 0;
        if (dur) this.audio.currentTime = p * dur;
      },
      destroy(){
        this.destroyed = true;
        this.stop();
        VOICE.players.delete(this);
      }
    };

    btn.onclick = () => player.playPause();
    canvas.addEventListener("click", (e)=> player.seekByClientX(e.clientX));
    audio.addEventListener("loadedmetadata", ()=>{ right.textContent = fmtClock(audio.duration || 0); });
    audio.addEventListener("play", ()=>{ player.setBtn(); if (!player.raf) player.tick(); });
    audio.addEventListener("pause", ()=>{
      player.setBtn();
      if (player.raf) cancelAnimationFrame(player.raf);
      player.raf = 0;
      const dur = audio.duration || 0;
      const cur = audio.currentTime || 0;
      const p = dur ? (cur/dur) : 0;
      if (player.peaks) drawWave(canvas, player.peaks, p);
    });
    audio.addEventListener("ended", ()=> player.stop());

    VOICE.players.add(player);
    player.init();
    return player;
  }

  // =========================
  // AUTH
  // =========================
  let authMode = "login";
  let authBusy = false;

  function showAuthError(msg){
    const el = $("authError");
    el.textContent = msg || "Ошибка";
    el.style.display = "block";
  }
  function hideAuthError(){
    const el = $("authError");
    el.textContent = "";
    el.style.display = "none";
  }
  function setAuthTab(mode){
    authMode = mode;
    $("tabLogin").classList.toggle("active", mode==="login");
    $("tabRegister").classList.toggle("active", mode==="register");
    $("btnAuthSubmit").textContent = (mode==="login") ? "Вход" : "Регистрация";
    hideAuthError();
  }
  function openAuth(mode="login"){
    setAuthTab(mode);
    modalManager.open("authOverlay");
    $("authUsername").value = me || $("authUsername").value || "";
    $("authPassword").value = "";
    setTimeout(()=> $("authUsername").focus(), 50);
  }
  function closeAuth(){
    modalManager.close("authOverlay");
  }
  async function authSubmit(){
    if (authBusy) return;
    hideAuthError();
    const username = $("authUsername").value.trim();
    const password = $("authPassword").value;
    if (!username) return showAuthError("Введите логин.");
    if (!password) return showAuthError("Введите пароль.");
    authBusy = true;
    $("btnAuthSubmit").disabled = true;
    $("btnAuthSubmit").classList.add("is-loading");
    $("btnAuthSubmit").textContent = (authMode==="login") ? "Входим…" : "Регистрируем…";
    try{
      const data = await api(authMode==="login" ? "/api/login" : "/api/register", "POST", {username, password});
      token = data.token;
      me = data.username;
      localStorage.setItem("token", token);
      localStorage.setItem("username", me);

      await refreshMe();
      setWhoami();
      requestNotificationPermissionIfNeeded();
      closeAuth();
      addSystem(`✅ Вход выполнен: ${me}`);

      connectWS_GLOBAL();
      await refreshChats(true);
      loadStories().catch(()=>{});
      maybeShowHelpOnboarding();
    }catch(e){
      const msg = String(e?.message || e || "");
      if (offlineMode || /интернет|недоступен|Failed to fetch/i.test(msg)){
        showNetworkError("Не удалось выполнить вход. Проверьте интернет и повторите попытку.");
      }
      showAuthError(`Не удалось войти: ${msg || "проверьте логин и пароль."}`);
    }finally{
      authBusy = false;
      $("btnAuthSubmit").disabled = false;
      $("btnAuthSubmit").classList.remove("is-loading");
      $("btnAuthSubmit").textContent = (authMode==="login") ? "Вход" : "Регистрация";
    }
  }

  async function refreshMe(){
    if (!token) return;
    const data = await api("/api/me");
    avatarUrl = data.avatar_url || "";
    displayName = data.display_name || "";
    profileBio = data.bio || "";
    localStorage.setItem("avatar_url", avatarUrl || "");
    localStorage.setItem("display_name", displayName || "");
    localStorage.setItem("profile_bio", profileBio || "");
  }

  // =========================
  // Typing (GLOBAL WS)
  // =========================
  const typingState = new Map(); // username -> bool (only for active chat)
  function renderTyping(){
    const names = [];
    for (const [u, on] of typingState.entries()){
      if (on && u !== me) names.push(u);
    }
    const typingEl = $("typing");
    if (!names.length){
      typingEl.classList.remove("active");
      typingEl.textContent = "";
      return;
    }
    const who = names.join(", ");
    const verb = names.length > 1 ? "печатают" : "печатает";
    typingEl.classList.add("active");
    typingEl.innerHTML = `${escapeHtml(`${who} ${verb}`)} <span class="typing-dots" aria-hidden="true"><span></span><span></span><span></span></span>`;
  }

  let typingTimer = null;
  let typingSent = false;

  function wsSendTyping(isTyping){
    if (!ws || ws.readyState !== 1) return;
    if (!activeChatId) return;
    try{
      ws.send(JSON.stringify({
        type: "typing",
        chat_id: activeChatId,
        is_typing: !!isTyping
      }));
    }catch(_){}
  }

  function onLocalTyping(){
    if (!token || !activeChatId) return;
    if (!typingSent){
      typingSent = true;
      wsSendTyping(true);
    }
    clearTimeout(typingTimer);
    typingTimer = setTimeout(()=>{
      typingSent = false;
      wsSendTyping(false);
    }, 900);
  }

  // =========================
  // Delivered ACK
  // =========================
  function wsSendDelivered(messageId, chatId){
    if (!ws || ws.readyState !== 1) return;
    try{
      ws.send(JSON.stringify({
        type: "delivered",
        message_id: Number(messageId),
        chat_id: String(chatId || activeChatId || "")
      }));
    }catch(_){}
  }

  // =========================
  // Read handling in UI (✓✓ read)
  // =========================
  function markMyMessagesReadUpTo(lastReadId){
    for (const [id, el] of msgElById.entries()){
      if (Number(id) <= Number(lastReadId) && el.dataset.sender === me){
        const st = el.querySelector('[data-role="status"]');
        if (st) st.textContent = "✓✓ read";
      }
    }
  }

  // =========================
  // GLOBAL WS connect
  // =========================
  function connectWS_GLOBAL(){
    if (!token) return;

    if (ws){
      try{ ws.close(); }catch{}
      ws = null;
    }

    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const since = Math.max(lastMsgId, loadLastMessageId());
    const url = `${proto}//${location.host}/ws/user?token=${encodeURIComponent(token)}&since=${encodeURIComponent(since)}`;

    setNet("connecting…");
    ws = new WebSocket(url);

    ws.onopen = () => {
      setOfflineMode(false);
      setNet("online");
    };
    ws.onclose = () => setNet(offlineMode ? "offline" : "не в сети");
    ws.onerror = () => setNet(offlineMode ? "offline" : "не в сети");

    ws.onmessage = (ev) => {
      let data = null;
      try{ data = JSON.parse(ev.data); }catch{ return; }

      // invited -> refresh chats
      if (data.type === "invited"){
        refreshChats(true).catch(()=>{});
        return;
      }

      if (data.type === "ping"){
        if (ws && ws.readyState === 1){
          try{
            ws.send(JSON.stringify({ type: "pong", ts: data.ts || Date.now() }));
          }catch(_){ }
        }
        return;
      }

      // typing (server sends for any chat)
      if (data.type === "typing"){
        if (data.chat_id === activeChatId){
          typingState.set(data.username, !!data.is_typing);
          renderTyping();
        }
        return;
      }

      // delivered event (server should rebroadcast to chat)
      if (data.type === "delivered"){
        const mid = Number(data.message_id || 0);
        const el = msgElById.get(mid);
        if (el && el.dataset.sender === me){
          const st = el.querySelector('[data-role="status"]');
          if (st && st.textContent.trim() === "✓") st.textContent = "✓✓";
        }
        return;
      }

      // read event (server broadcasts on mark_read)
      if (data.type === "read"){
        // For DM: when other read, mark our messages read.
        // For groups: simple behavior — if anyone (not me) read up to X, mark ours read up to X.
        if (data.chat_id === activeChatId && data.username !== me){
          markMyMessagesReadUpTo(Number(data.last_read_id || 0));
        }
        // update chat list (unread may change)
        refreshChats(false).catch(()=>{});
        return;
      }

      // new message for ANY chat
      if (data.type === "message"){
        if (data.sender && data.sender !== me && !isChatMuted(data.chat_id)){
          beep();
          if (document.visibilityState !== "visible" && "Notification" in window && Notification.permission === "granted"){
            new Notification(`Новое сообщение: ${data.sender}`, { body: (data.text || "[media]").slice(0,80) });
          }
        }

        // ACK delivered for received messages (not mine)
        if (data.sender && data.sender !== me){
          wsSendDelivered(data.id, data.chat_id);
        }

        if (data.chat_id === activeChatId){
          addMsg(data);
        }

        refreshChats(false).catch(()=>{});
        return;
      }

      if (data.type === "call_offer"){
        if (data.username !== me) handleIncomingOffer(data).catch(()=>{});
        return;
      }
      if (data.type === "call_answer"){
        if (data.username === me) return;
        if (callState.active && callState.callId === String(data.call_id || "") && callState.status === "dialing"){
          stopCallTimers();
          callState.status = "connected";
          callState.connectedAt = Math.floor(Date.now()/1000);
          callUi.hint.textContent = "Разговор: 0:00";
          updateCallUi();
          startCallTicker();
          setStatus("✅ Разговор начат");
        }
        return;
      }
      if (data.type === "call_reject"){
        if (data.username === me) return;
        const callId = String(data.call_id || "");
        const pending = pendingIncomingCalls.get(callId);
        if (pending){
          pendingIncomingCalls.delete(callId);
          pushCallLog({ kind:pending.mode, status:"rejected", started_at:pending.startedAt, duration:0 }, pending.chatId);
        }
        if (callState.active && callState.callId === callId){
          const ended = { ...callState };
          endCall({ silent:true, emit:false });
          pushCallLog({ kind:ended.mode, status:"rejected", started_at:ended.startedAt, duration:0 }, ended.chatId);
          addSystem("☎️ Собеседник отклонил звонок.");
        }
        return;
      }
      if (data.type === "call_timeout"){
        if (data.username === me) return;
        const callId = String(data.call_id || "");
        const pending = pendingIncomingCalls.get(callId);
        if (pending){
          pendingIncomingCalls.delete(callId);
          pushCallLog({ kind:pending.mode, status:"missed", started_at:pending.startedAt, duration:0 }, pending.chatId);
          if (pending.chatId === activeChatId) addSystem("☎️ Пропущенный звонок.");
        }
        if (callState.active && callState.callId === callId){
          const ended = { ...callState };
          endCall({ silent:true, emit:false });
          pushCallLog({ kind:ended.mode, status:"missed", started_at:ended.startedAt, duration:0 }, ended.chatId);
          addSystem("☎️ Пропущенный звонок.");
        }
        return;
      }
      if (data.type === "call_end"){
        if (data.username === me) return;
        if (callState.active && callState.callId === String(data.call_id || "")){
          const duration = Number(data.duration || 0);
          const ended = { ...callState };
          endCall({ silent:true, emit:false });
          pushCallLog({ kind:data.mode || ended.mode, status:"ended", started_at:Math.floor(Date.now()/1000)-duration, duration }, ended.chatId);
          addSystem("☎️ Звонок завершён собеседником.");
        }
        return;
      }

      // edited
      if (data.type === "message_edited"){
        if (data.chat_id === activeChatId){
          applyEdited(data.id, data.text, true, data.edited_at || null);
        }
        return;
      }

      // deleted for all
      if (data.type === "message_deleted_all"){
        if (data.chat_id === activeChatId){
          applyDeletedAll(data.id, data.deleted_at || null);
        }
        refreshChats(false).catch(()=>{});
        return;
      }


      if (data.type === "reaction_added" || data.type === "reaction_removed"){
        if (data.chat_id === activeChatId){
          applyReactionEvent(data.message_id, data.emoji, data.username, data.type === "reaction_added");
        }
        return;
      }

      if (data.type === "pin_added" || data.type === "pin_removed"){
        if (data.chat_id === activeChatId){
          loadPins().catch(()=>{});
        }
        return;
      }

      if (data.type === "member_removed" || data.type === "role_updated"){
        refreshChats(false).catch(()=>{});
        return;
      }

      // chat deleted
      if (data.type === "chat_deleted"){
        refreshChats(true).catch(()=>{});
        if (data.chat_id === activeChatId){
          $("msgs").innerHTML = "";
          addSystem("🗑 Этот чат удалён.");
        }
        return;
      }
    };
  }

  // =========================
  // Sheets
  // =========================
  const sheet = {
    overlay: $("sheetOverlay"),
    title: $("sheetTitle"),
    input: $("sheetInput"),
    hint: $("sheetHint"),
    ok: $("btnSheetOk"),
    mode: null
  };
  function openSheet(mode){
    if (!token) return openAuth("login");
    sheet.mode = mode;
    modalManager.open("sheetOverlay");
    if (mode === "group"){
      sheet.title.textContent = "Новый групповой чат";
      sheet.input.placeholder = "Название (например: work)";
      sheet.hint.textContent = "Создаст чат и переключит тебя в него.";
      sheet.ok.textContent = "Создать";
    } else {
      sheet.title.textContent = "Новый личный чат";
      sheet.input.placeholder = "Логин пользователя";
      sheet.hint.textContent = "Пользователь должен быть зарегистрирован.";
      sheet.ok.textContent = "Открыть чат";
    }
    sheet.input.value = "";
    setTimeout(()=> sheet.input.focus(), 50);
  }
  function closeSheet(){
    modalManager.close("sheetOverlay");
    sheet.mode = null;
  }

  const profile = { overlay: $("profileOverlay") };
  const help = { overlay: $("helpOverlay") };
  const userProfile = { overlay: $("userProfileOverlay"), activeUsername: "" };
  const profileMenu = { root: $("profileMenu"), trigger: $("whoami") };

  function openProfileMenu(){
    if (!token) return openAuth("login");
    profileMenu.root.classList.add("open");
    profileMenu.root.setAttribute("aria-hidden", "false");
    profileMenu.trigger.setAttribute("aria-expanded", "true");
  }

  function closeProfileMenu(){
    profileMenu.root.classList.remove("open");
    profileMenu.root.setAttribute("aria-hidden", "true");
    profileMenu.trigger.setAttribute("aria-expanded", "false");
  }

  function toggleProfileMenu(){
    if (profileMenu.root.classList.contains("open")) closeProfileMenu();
    else openProfileMenu();
  }
  function openProfile(){
    if (!token) return openAuth("login");
    $("profileHint").textContent = `@${me}`;
    $("profileDisplayName").value = displayName || "";
    $("profileBio").value = profileBio || "";
    modalManager.open("profileOverlay");
    loadStories().catch(()=>{});
    loadAvatarHistory().catch(()=>{});
  }
  function closeProfile(){
    modalManager.close("profileOverlay");
  }

  function openHelp(){
    modalManager.open("helpOverlay");
    localStorage.setItem(HELP_ONBOARDING_KEY, "1");
  }

  function closeHelp(){
    modalManager.close("helpOverlay");
  }

  function maybeShowHelpOnboarding(){
    if (localStorage.getItem(HELP_ONBOARDING_KEY) === "1") return;
    openHelp();
  }

  function closeUserProfile(){
    modalManager.close("userProfileOverlay");
    userProfile.activeUsername = "";
  }

  async function openUserProfile(username){
    const target = String(username || "").trim();
    if (!target) return;
    if (!token) return openAuth("login");
    try{
      const data = await api(`/api/users/${encodeURIComponent(target)}/profile`);
      const user = data.user || {};
      const profileName = user.display_name || user.username || target;
      userProfile.activeUsername = target;
      $("userProfileTitle").textContent = profileName;
      $("userProfileHandle").textContent = `@${user.username || target}`;
      $("userProfileBio").textContent = user.bio || "Без описания";
      $("userProfileAvatar").src = user.avatar_url || "";
      $("userProfileAvatar").style.display = user.avatar_url ? "block" : "none";

      const storiesBox = $("userProfileStories");
      const userStories = data.stories || [];
      storiesBox.innerHTML = "";
      if (!userStories.length){
        storiesBox.innerHTML = '<div class="small">Активных историй нет</div>';
      } else {
        for (const story of userStories){
          const row = document.createElement("div");
          row.className = "chatitem profile-story";

          const info = document.createElement("div");
          info.innerHTML = `<div class="title">${escapeHtml(story.caption || 'История')}</div><div class="sub">${new Date((story.created_at||0)*1000).toLocaleString()}</div>`;
          row.appendChild(info);

          const kind = inferMediaKind(story.media_url, story.media_kind || "");
          if (story.media_url && kind === "video"){
            const video = document.createElement("video");
            video.className = "profile-story-video";
            video.src = story.media_url;
            video.controls = true;
            video.preload = "metadata";
            video.playsInline = true;
            row.appendChild(video);
          } else if (story.media_url){
            const img = document.createElement("img");
            img.className = "profile-story-media";
            img.src = story.media_url;
            img.alt = story.caption || "История";
            img.loading = "lazy";
            row.appendChild(img);
          }

          if (story.media_url){
            const openBtn = document.createElement("button");
            openBtn.className = "btn btn--secondary";
            openBtn.type = "button";
            openBtn.textContent = "Открыть в плеере";
            openBtn.onclick = () => openMediaViewer(story.media_url, kind, story.caption || "История");
            row.appendChild(openBtn);
          }

          if (data.can_manage){
            const del = document.createElement("button");
            del.className = "trash iconbtn";
            del.textContent = "🗑";
            del.title = "Удалить историю";
            del.setAttribute("aria-label", "Удалить историю");
            del.onclick = async (e)=>{
              e.stopPropagation();
              await api(`/api/stories/${story.id}`, "DELETE");
              await openUserProfile(target);
              if (target === me) await loadStories();
            };
            row.appendChild(del);
          }
          storiesBox.appendChild(row);
        }
      }

      const avatarBox = $("userProfileAvatars");
      const avatarItems = [];
      if (user.avatar_url){
        avatarItems.push({ id: null, avatar_url: user.avatar_url, created_at: Math.floor(Date.now()/1000), current: true });
      }
      for (const item of (data.avatar_history || [])) avatarItems.push(item);
      avatarBox.innerHTML = "";
      if (!avatarItems.length){
        avatarBox.innerHTML = '<div class="small">Аватаров пока нет</div>';
      } else {
        for (const item of avatarItems){
          const row = document.createElement("div");
          row.className = "chatitem";
          row.innerHTML = `<div><div class="title">${item.current ? 'Текущий аватар' : 'Аватар'}</div><div class="sub">${item.current ? 'актуальный' : new Date((item.created_at||0)*1000).toLocaleString()}</div></div>`;
          row.onclick = () => openMediaViewer(item.avatar_url, "image", item.current ? "Текущий аватар" : "Аватар");
          if (data.can_manage && item.id){
            const del = document.createElement("button");
            del.className = "trash iconbtn";
            del.textContent = "🗑";
            del.title = "Удалить из истории";
            del.setAttribute("aria-label", "Удалить из истории");
            del.onclick = async (e)=>{
              e.stopPropagation();
              await api(`/api/avatar/history/${item.id}`, "DELETE");
              await openUserProfile(target);
              if (target === me) await loadAvatarHistory();
            };
            row.appendChild(del);
          }
          avatarBox.appendChild(row);
        }
      }

      modalManager.open("userProfileOverlay");
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  }

  // =========================
  // Voice hold-to-record + preview
  // =========================
  const recBtn = $("btnRecHold");
  let rec = { active:false, mr:null, chunks:[], stream:null };
  let preview = { blob:null, file:null, url:"" };

  function openVoicePreview(){
    modalManager.open("voicePreviewOverlay");
    const holder = $("voicePreviewPlayer");
    holder.innerHTML = "";
    const player = createVoicePlayer(preview.url);
    holder.appendChild(player.root);
    $("voicePreviewHint").textContent = "Прослушай и отправь / отмена.";
  }
  function closeVoicePreview(){
    modalManager.close("voicePreviewOverlay");
    $("voicePreviewPlayer").innerHTML = "";
  }
  function clearPreview(){
    try{ if (preview.url) URL.revokeObjectURL(preview.url); }catch(_){}
    preview = { blob:null, file:null, url:"" };
  }

  async function startHoldRec(){
    if (!token) return openAuth("login");
    if (!activeChatId) return addSystem("⚠️ Сначала выбери чат.");
    if (!navigator.mediaDevices?.getUserMedia) return addSystem("⚠️ Нет доступа к микрофону.");
    if (!window.MediaRecorder) return addSystem("⚠️ MediaRecorder не поддерживается.");
    try{
      clearPreview();
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      const mr = new MediaRecorder(stream);

      rec.active = true;
      rec.mr = mr;
      rec.stream = stream;
      rec.chunks = [];

      recBtn.classList.add("hold");
      setStatus("🎙 Recording…");

      mr.ondataavailable = (e) => { if (e.data?.size) rec.chunks.push(e.data); };
      mr.onstop = () => {
        try{
          const blob = new Blob(rec.chunks, { type: mr.mimeType || "audio/webm" });
          const file = new File([blob], `voice_${Date.now()}.webm`, { type: blob.type });
          preview.blob = blob;
          preview.file = file;
          preview.url = URL.createObjectURL(blob);
          openVoicePreview();
        }catch(e){
          addSystem("❌ " + (e.message || e));
        }finally{
          try{ rec.stream?.getTracks().forEach(t => t.stop()); }catch(_){}
          rec.stream = null; rec.mr = null; rec.chunks = [];
        }
      };

      mr.start(200);
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  }
  function stopHoldRec(){
    if (!rec.active) return;
    rec.active = false;
    recBtn.classList.remove("hold");
    setStatus("—");
    try{ rec.mr?.stop(); }catch(_){}
  }

  async function uploadMedia(file, captionOverride=null){
    if (!token) return openAuth("login");
    if (!activeChatId) return addSystem("⚠️ Сначала выбери чат.");
    const caption = captionOverride !== null ? captionOverride : $("text").value.trim();
    const fd = new FormData();
    fd.append("chat_id", activeChatId);
    fd.append("text", caption);
    fd.append("file", file);
    setStatus("⏫ Upload…");
    await request("/api/upload", { method: "POST", body: fd, auth: true });
    $("text").value = "";
    $("file").value = "";
    setStatus(`online • ${activeChatTitle}`);
  }

  async function sendVoiceNow(){
    if (!preview.file) return;
    try{
      setStatus("⏫ Upload voice…");
      await uploadMedia(preview.file, "");
      closeVoicePreview();
      clearPreview();
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }finally{
      setStatus(`online • ${activeChatTitle}`);
    }
  }

  // =========================
  // Avatar upload
  // =========================
  async function uploadAvatar(){
    const f = $("avatarFile").files && $("avatarFile").files[0];
    if (!f) return $("profileHint").textContent = "Выбери файл.";
    try{
      $("profileHint").textContent = "Uploading…";
      const fd = new FormData();
      fd.append("file", f);
      const data = await request("/api/avatar", { method: "POST", body: fd, auth: true });
      avatarUrl = data.avatar_url || "";
      localStorage.setItem("avatar_url", avatarUrl || "");
      setWhoami();
      $("profileHint").textContent = "✅ Готово!";
    }catch(e){
      $("profileHint").textContent = "❌ " + (e.message || e);
    }
  }

  // =========================
  // Chats + unread
  // =========================
  function isChatMuted(chatId){
    const c = chats.find(x => x.id === chatId);
    if (!c || !c.muted_until) return false;
    return Number(c.muted_until) > Math.floor(Date.now()/1000);
  }

  function computeChatTitle(c){
    let title = c.title || c.id;
    if (String(c.id || "") === `fav:${me}`) return "Избранное";
    if (c.type === "dm" && c.title && c.title.startsWith("dm:")){
      const parts = c.title.slice(3).split("|");
      if (parts.length === 2){
        const other = parts[0] === me ? parts[1] : parts[0];
        title = `ЛС: ${other}`;
      }
    }
    return title;
  }
  function canDeleteChat(c){
    if (String(c.id || "") === `fav:${me}`) return false;
    if (c.type === "dm") return true;
    return (c.created_by === me);
  }

  function getFilteredChats(){
    const q = ($("chatSearch")?.value || "").trim().toLowerCase();
    return chats.filter((c)=>{
      const title = computeChatTitle(c).toLowerCase();
      const lastText = String(c.last_text || "").toLowerCase();
      const sender = String(c.last_sender || "").toLowerCase();
      const passFilter = activeChatFilter === "all"
        || (activeChatFilter === "dm" && c.type === "dm")
        || (activeChatFilter === "group" && c.type === "group");
      const passSearch = !q || title.includes(q) || lastText.includes(q) || sender.includes(q);
      return passFilter && passSearch;
    });
  }

  function renderChatListSkeleton(count = 6){
    const list = $("chatlist");
    list.innerHTML = "";
    for (let i = 0; i < count; i += 1){
      const row = document.createElement("div");
      row.className = "chatitem skeleton-row";
      row.setAttribute("aria-hidden", "true");
      row.innerHTML = `
        <div class="chat-avatar skeleton-block"></div>
        <div class="left">
          <div class="skeleton-line skeleton-line-title"></div>
          <div class="skeleton-line"></div>
        </div>
      `;
      list.appendChild(row);
    }
  }

  function renderChatListError(message, onRetry){
    const list = $("chatlist");
    list.innerHTML = "";
    const wrap = document.createElement("div");
    wrap.className = "state-block";
    wrap.innerHTML = `
      <div class="state-title">Не удалось загрузить чаты</div>
      <div class="state-sub">${escapeHtml(message || "Проверьте интернет и повторите попытку.")}</div>
    `;
    const retryBtn = document.createElement("button");
    retryBtn.type = "button";
    retryBtn.className = "btn state-btn";
    retryBtn.textContent = "Повторить";
    retryBtn.onclick = () => onRetry && onRetry();
    wrap.appendChild(retryBtn);
    list.appendChild(wrap);
  }

  function renderMessagesSkeleton(count = 7){
    const box = $("msgs");
    box.innerHTML = "";
    for (let i = 0; i < count; i += 1){
      const row = document.createElement("div");
      row.className = "msg-row" + (i % 3 === 0 ? " me" : "");
      row.setAttribute("aria-hidden", "true");

      const bubble = document.createElement("div");
      bubble.className = "msg msg-skeleton";
      bubble.innerHTML = `
        <div class="skeleton-line skeleton-line-title"></div>
        <div class="skeleton-line"></div>
        <div class="skeleton-line skeleton-line-short"></div>
      `;
      row.appendChild(bubble);
      box.appendChild(row);
    }
  }

  function renderMessagesError(message, onRetry){
    const box = $("msgs");
    box.innerHTML = "";
    const wrap = document.createElement("div");
    wrap.className = "state-block state-block-chat";
    wrap.innerHTML = `
      <div class="state-title">Не удалось загрузить сообщения</div>
      <div class="state-sub">${escapeHtml(message || "Проверьте интернет и попробуйте снова.")}</div>
    `;
    const retryBtn = document.createElement("button");
    retryBtn.type = "button";
    retryBtn.className = "btn state-btn";
    retryBtn.textContent = "Повторить";
    retryBtn.onclick = () => onRetry && onRetry();
    wrap.appendChild(retryBtn);
    box.appendChild(wrap);
  }

  function renderChatList(){
    const list = $("chatlist");
    list.innerHTML = "";
    const visibleChats = getFilteredChats();
    if (!visibleChats.length){
      if (chats.length){
        const div = document.createElement("div");
        div.className = "small";
        div.textContent = "Ничего не найдено";
        list.appendChild(div);
      } else {
        const wrap = document.createElement("div");
        wrap.className = "state-block";
        wrap.innerHTML = `
          <div class="state-title">Пока нет чатов</div>
          <div class="state-sub">Создайте личный или групповой чат, чтобы начать общение.</div>
        `;
        const ctaBtn = document.createElement("button");
        ctaBtn.type = "button";
        ctaBtn.className = "btn state-btn";
        ctaBtn.textContent = "Создать";
        ctaBtn.onclick = () => openSheet("group");
        wrap.appendChild(ctaBtn);
        list.appendChild(wrap);
      }
      return;
    }

    for (const c of visibleChats){
      const item = document.createElement("div");
      item.className = "chatitem" + (c.id === activeChatId ? " active" : "");

      const avatar = document.createElement("div");
      avatar.className = "chat-avatar";
      const title = computeChatTitle(c).replace(/^ЛС:\s*/, "").trim();
      avatar.textContent = (title[0] || "#").toUpperCase();

      const left = document.createElement("div");
      left.className = "left";

      const t1 = document.createElement("div");
      t1.className = "title";
      t1.textContent = title;

      const t2 = document.createElement("div");
      t2.className = "sub";
      if (c.last_text){
        const sender = c.last_sender === me ? "Ты" : c.last_sender;
        t2.textContent = `${sender}: ${String(c.last_text)}`;
      } else {
        t2.textContent = "Нет сообщений";
      }

      left.appendChild(t1);
      left.appendChild(t2);

      const right = document.createElement("div");
      right.className = "chat-right";

      const meta = document.createElement("div");
      meta.className = "chat-meta";

      const time = document.createElement("div");
      time.className = "chat-time";
      time.textContent = c.last_created_at ? fmtTs(c.last_created_at) : "";
      meta.appendChild(time);

      const badges = document.createElement("div");
      badges.className = "chat-badges";

      if (c.unread && Number(c.unread) > 0 && c.id !== activeChatId){
        const u = document.createElement("span");
        u.className = "unread";
        u.textContent = String(c.unread);
        badges.appendChild(u);
      }

      if (isChatMuted(c.id)){
        const m = document.createElement("span");
        m.className = "badge";
        m.textContent = "🔕";
        badges.appendChild(m);
      }

      meta.appendChild(badges);
      right.appendChild(meta);

      const actions = document.createElement("div");
      actions.className = "chat-actions";

      if (canDeleteChat(c)){
        const del = document.createElement("button");
        del.type = "button";
        del.className = "trash iconbtn";
        del.textContent = "🗑";
        del.title = "Удалить чат";
        del.setAttribute("aria-label", "Удалить чат");
        del.onclick = async (e) => {
          e.stopPropagation();
          if (!confirm(`Удалить чат "${title}"?`)) return;
          try{
            await api(`/api/chats/${encodeURIComponent(c.id)}`, "DELETE");
            addSystem(`🗑 Deleted: ${title}`);
            await refreshChats(true);
          }catch(err){
            addSystem("❌ " + (err.message || err));
          }
        };
        actions.appendChild(del);
      }

      if (actions.childElementCount) right.appendChild(actions);

      item.appendChild(avatar);
      item.appendChild(left);
      item.appendChild(right);

      item.onclick = () => {
        selectChat(c.id);
        if (isMobile()) closeSidebar();
      };

      list.appendChild(item);
    }
  }

  async function refreshChats(selectIfNeeded){
    if (!token){ openAuth("login"); return; }
    renderChatListSkeleton();
    try{
      const data = await api("/api/chats");
      chats = (data.chats || []);
      renderChatList();

      if (!chats.length){
        activeChatId = "";
        activeChatTitle = "";
        activeChatType = "";
        activeChatCreatedBy = "";
        localStorage.removeItem("activeChatId");
        $("msgs").innerHTML = "";
        addSystem("ℹ️ У тебя пока нет чатов. Создай групповой или личный чат.");
        setStatus("");
        updateChatActionState();
        return;
      }

      if (selectIfNeeded){
        if (!activeChatId || !chats.find(c => c.id === activeChatId)){
          activeChatId = chats[0].id;
          localStorage.setItem("activeChatId", activeChatId);
        }
        selectChat(activeChatId);
      } else {
        renderChatList();
      }
    }catch(e){
      const msg = String(e?.message || e || "");
      showNetworkError("Не удалось загрузить список чатов.");
      addSystem(`❌ Ошибка загрузки чатов: ${msg || "повторите попытку."}`);
      renderChatListError(msg, () => refreshChats(selectIfNeeded));
      throw e;
    }
  }

  function selectChat(chatId){
    stopAllExcept(null);
    endCall({ silent:true });
    clearReply();
    msgElById.clear();
    lastMsgId = loadLastMessageId();
    oldestLoadedMessageId = null;
    hasMoreHistory = true;
    isHistoryLoading = false;

    typingState.clear();
    renderTyping();

    activeChatId = chatId;
    localStorage.setItem("activeChatId", activeChatId);

    const found = chats.find(c => c.id === chatId);
    activeChatTitle = found ? computeChatTitle(found) : chatId;
    activeChatType = found ? (found.type || "") : "";
    activeChatCreatedBy = found ? (found.created_by || "") : "";

    $("msgs").innerHTML = "";
    $("chatHeadTitle").textContent = activeChatTitle || "Чат";
    $("chatHeadSub").textContent = activeChatType === "group" ? "Групповой чат" : "Личный чат";
    addSystem(`📌 Chat: ${activeChatTitle}`);

    updateChatActionState();
    renderChatList();
    loadHistory();
    loadPins().catch(()=>{});
  }

  async function createGroupChat(title){
    const t = (title || "").trim();
    if (!t) return addSystem("⚠️ Введи название чата.");
    const data = await api("/api/chats", "POST", { title: t });
    await refreshChats(false);
    selectChat(data.chat.id);
    addSystem(`✅ Created: ${data.chat.title}`);
  }
  async function createDM(user){
    const u = (user || "").trim();
    if (!u) return addSystem("⚠️ Введи логин.");
    const data = await api("/api/chats/dm", "POST", { username: u });
    await refreshChats(false);
    selectChat(data.chat.id);
    addSystem(`✅ ${data.chat.title}`);
  }

  // =========================
  // Messages: render/edit/delete
  // =========================
  function addMsg(m, opts = {}){
    const prepend = !!opts.prepend;
    if (isSystemSender(m)){
      if (opts.notifySystem !== false){
        showSystemToast(m.text || "Системное сообщение");
      }
      return;
    }

    const box = $("msgs");
    const row = document.createElement("div");
    row.className = "msg-row" + ((m.sender === me) ? " me" : "");

    const stack = document.createElement("div");
    stack.className = "msg-stack" + ((m.sender === me) ? " me" : "");
    stack.dataset.msgId = String(m.id || "");
    stack.dataset.sender = String(m.sender || "");
    stack.dataset.deletedForAll = String(!!m.deleted_for_all);
    stack.dataset.editedAt = String(m.edited_at || "");
    stack.dataset.deletedAt = String(m.deleted_at || "");
    stack.dataset.myReactions = JSON.stringify(m.my_reactions || []);

    const div = document.createElement("div");
    div.className = "msg" + ((m.sender === me) ? " me" : "");

    const meta = document.createElement("div");
    meta.className = "meta";

    const messageAuthor = String(m.sender || m.username || "").trim();

    const left = document.createElement("button");
    left.type = "button";
    left.className = "sender-link";
    left.textContent = messageAuthor || "—";
    left.onclick = () => openUserProfile(messageAuthor);
    meta.appendChild(left);
    div.appendChild(meta);

    const body = document.createElement("div");
    body.dataset.role = "body";

    if (m.deleted_for_all){
      body.className = "deleted";
      body.textContent = "Это сообщение удалено";
      div.appendChild(body);
    } else {
      const media_url = m.media_url || "";
      const media_kind = m.media_kind || "";
      const media_name = m.media_name || "";
      const text = (m.text || "").trim();

      if (m.reply_to_id){
        const rep = document.createElement("div");
        rep.className = "reply-preview";
        rep.dataset.replyToId = String(m.reply_to_id);
        const t = (m.reply_text || "").trim();
        const replySender = String(m.reply_sender || "user").trim();
        const nickBtn = document.createElement("button");
        nickBtn.type = "button";
        nickBtn.className = "sender-link";
        nickBtn.textContent = `↪ ${replySender}`;
        nickBtn.onclick = (e) => {
          e.stopPropagation();
          openUserProfile(replySender);
        };
        const previewText = document.createElement("span");
        previewText.textContent = `: ${t ? t.slice(0,80) : "[media]"}`;
        rep.appendChild(nickBtn);
        rep.appendChild(previewText);
        rep.onclick = () => jumpToMessage(Number(m.reply_to_id));
        div.appendChild(rep);
      }

      if (media_url && media_kind === "image"){
        const wrap = document.createElement("div");
        wrap.className = "media";
        const img = document.createElement("img");
        img.src = media_url;
        img.alt = media_name || "image";
        img.loading = "lazy";
        img.style.cursor = "pointer";
        img.onclick = () => openMediaViewer(media_url, "image", media_name || "Фото");
        wrap.appendChild(img);
        div.appendChild(wrap);
      }

      if (media_url && media_kind === "video"){
        const wrap = document.createElement("div");
        wrap.className = "media";
        const video = document.createElement("video");
        video.src = media_url;
        video.controls = true;
        video.preload = "metadata";
        video.addEventListener("dblclick", () => openMediaViewer(media_url, "video", media_name || "Видео"));
        wrap.appendChild(video);
        div.appendChild(wrap);
      }

      if (media_url && media_kind === "audio"){
        const wrap = document.createElement("div");
        wrap.className = "media";
        wrap.style.padding = "10px";
        const player = createVoicePlayer(media_url);
        wrap.appendChild(player.root);

        div.appendChild(wrap);
      }

      const isCallLog = renderCallLog(body, m);
      if (!isCallLog){
        body.textContent = text;
        body.style.marginTop = (media_url ? "8px" : "0");
      }
      div.appendChild(body);
    }

    const footer = document.createElement("div");
    footer.className = "msg-footer";

    // ticks for my messages
    if (m.sender === me){
      const st = document.createElement("div");
      st.className = "ticks";
      st.dataset.role = "status";
      st.textContent = "✓"; // sent
      footer.appendChild(st);
    }

    const timeMeta = document.createElement("span");
    timeMeta.className = "meta-time";
    timeMeta.dataset.role = "meta-time";
    timeMeta.textContent = fmtTs(m.created_at);

    if (m.is_edited){
      const ed = document.createElement("span");
      ed.className = "edited";
      ed.textContent = "изменено";
      timeMeta.appendChild(ed);
    }
    footer.appendChild(timeMeta);
    div.appendChild(footer);

    stack.appendChild(div);

    const reacts = document.createElement("div");
    reacts.className = "reactions";
    reacts.dataset.role = "reactions";
    renderReactions(reacts, m.id, m.reactions || {}, m.my_reactions || []);
    stack.appendChild(reacts);

    const actionCol = document.createElement("div");
    actionCol.className = "msg-actions-col";
    const addBtn = document.createElement("button");
    addBtn.className = "react msg-quick-add";
    addBtn.type = "button";
    addBtn.textContent = "+";
    addBtn.setAttribute("aria-label", "Добавить реакцию");
    addBtn.onclick = (e) => {
      e.stopPropagation();
      openEmojiPicker(m.id, addBtn);
    };
    actionCol.appendChild(addBtn);

    // context menu
    stack.addEventListener("contextmenu", (e)=>{
      e.preventDefault();
      openCtxForMsg(stack, e.clientX, e.clientY);
    });

    let lpTimer = null;
    stack.addEventListener("pointerdown", (e)=>{
      if (e.pointerType === "mouse") return;
      lpTimer = setTimeout(()=> openCtxForMsg(stack, e.clientX, e.clientY), 520);
    });
    stack.addEventListener("pointerup", ()=> { if (lpTimer) clearTimeout(lpTimer); lpTimer = null; });
    stack.addEventListener("pointercancel", ()=> { if (lpTimer) clearTimeout(lpTimer); lpTimer = null; });

    const messageId = Number(m.id || 0);
    if (messageId && msgElById.has(messageId)) return;

    msgElById.set(messageId, stack);
    if (messageId){
      oldestLoadedMessageId = oldestLoadedMessageId === null ? messageId : Math.min(oldestLoadedMessageId, messageId);
    }
    lastMsgId = Math.max(lastMsgId, messageId);
    persistLastMessageId(lastMsgId);

    const avatarNode = createMessageAvatar(messageAuthor, messageAuthor === me, m.sender_avatar_url);
    avatarNode.classList.add("clickable");
    avatarNode.onclick = () => openUserProfile(messageAuthor);
    if (messageAuthor === me){
      row.appendChild(stack);
      row.appendChild(actionCol);
      row.appendChild(avatarNode);
    } else {
      row.appendChild(avatarNode);
      row.appendChild(actionCol);
      row.appendChild(stack);
    }

    const stick = isNearBottom(box);
    if (prepend){
      box.prepend(row);
      return;
    }
    const animate = opts.animate !== false;
    if (animate) row.classList.add("msg-row-enter");
    box.appendChild(row);
    if (stick) {
      scrollToBottom(box);
      maybeMarkRead();
    }
    updateToBottom();
  }

  function renderReactions(holder, messageId, reactionMap, myReactions){
    holder.innerHTML = "";
    for (const [emoji, cnt] of Object.entries(reactionMap || {})){
      const rb = document.createElement("button");
      rb.className = "react";
      rb.dataset.emoji = emoji;
      if ((myReactions || []).includes(emoji)) rb.classList.add("mine");
      rb.textContent = `${emoji} ${cnt}`;
      rb.onclick = async (e) => {
        e.stopPropagation();
        await toggleReaction(messageId, emoji);
      };
      holder.appendChild(rb);
    }
  }

  function openEmojiPicker(messageId, anchor){
    const existing = document.querySelector(".emoji-picker");
    if (existing) existing.remove();
    const picker = document.createElement("div");
    picker.className = "emoji-picker";
    for (const em of REACTION_EMOJIS){
      const b = document.createElement("button");
      b.type = "button";
      b.textContent = em;
      b.onclick = async (e)=>{
        e.stopPropagation();
        picker.remove();
        await toggleReaction(messageId, em);
      };
      picker.appendChild(b);
    }
    document.body.appendChild(picker);
    const r = anchor.getBoundingClientRect();
    picker.style.left = `${Math.max(8, r.left)}px`;
    picker.style.top = `${Math.max(8, r.top - 52)}px`;
    setTimeout(()=>{
      const close = (ev)=>{
        if (!picker.contains(ev.target)){
          picker.remove();
          document.removeEventListener("click", close);
        }
      };
      document.addEventListener("click", close);
    }, 0);
  }

  function applyEdited(id, text, isEdited, editedAt = null){
    const el = msgElById.get(id);
    if (!el) return;
    if (el.dataset.deletedForAll === "true") return;

    if (editedAt){
      el.dataset.editedAt = String(editedAt);
    }

    const body = el.querySelector('[data-role="body"]');
    if (body) body.textContent = text;

    const metaTime = el.querySelector('[data-role="meta-time"]');
    if (metaTime && isEdited && !metaTime.querySelector(".edited")){
      const ed = document.createElement("span");
      ed.className = "edited";
      ed.textContent = "изменено";
      metaTime.appendChild(ed);
    }
  }

  function applyDeletedAll(id, deletedAt = null){
    const el = msgElById.get(id);
    if (!el) return;
    el.dataset.deletedForAll = "true";
    if (deletedAt){
      el.dataset.deletedAt = String(deletedAt);
    }
    el.querySelectorAll(".media").forEach(n => n.remove());
    const body = el.querySelector('[data-role="body"]');
    if (body){
      body.className = "deleted";
      body.textContent = "Это сообщение удалено";
    }
  }

  async function loadHistory(){
    if (!token) return openAuth("login");
    if (!activeChatId) return;

    renderMessagesSkeleton();
    msgElById.clear();
    lastMsgId = loadLastMessageId();
    oldestLoadedMessageId = null;
    hasMoreHistory = true;

    try{
      const data = await api(`/api/messages?chat_id=${encodeURIComponent(activeChatId)}&limit=50`);
      const box = $("msgs");
      box.innerHTML = "";
      for (const m of (data.messages || [])) addMsg(m, { notifySystem: false, animate: false });
      hasMoreHistory = Boolean(data.has_more);
      scrollToBottom(box);
      updateToBottom();
      maybeMarkRead();
      refreshChats(false).catch(()=>{});
    } catch (e) {
      const msg = String(e?.message || e || "");
      showNetworkError("Не удалось загрузить сообщения чата.");
      addSystem(`❌ Ошибка загрузки сообщений: ${msg || "повторите попытку."}`);
      renderMessagesError(msg, () => loadHistory());
    }
  }

  async function loadOlderMessages(){
    if (!token || !activeChatId) return;
    if (!hasMoreHistory || isHistoryLoading) return;
    if (!oldestLoadedMessageId) return;

    const box = $("msgs");
    const prevHeight = box.scrollHeight;
    const prevTop = box.scrollTop;
    isHistoryLoading = true;

    try{
      const data = await api(`/api/messages?chat_id=${encodeURIComponent(activeChatId)}&before_id=${oldestLoadedMessageId}&limit=50`);
      const items = data.messages || [];
      for (let i = items.length - 1; i >= 0; i -= 1){
        addMsg(items[i], { notifySystem: false, prepend: true });
      }
      hasMoreHistory = Boolean(data.has_more);
      const nextHeight = box.scrollHeight;
      box.scrollTop = prevTop + (nextHeight - prevHeight);
    }catch(_){
      showNetworkError("Не удалось подгрузить историю чата.");
    }finally{
      isHistoryLoading = false;
    }
  }

  async function send(){
    const text = $("text").value.trim();
    if (!text) return;
    if (!token) return openAuth("login");
    if (!activeChatId) return;

    $("text").value = "";
    try{
      await api("/api/messages", "POST", { chat_id: activeChatId, text, reply_to_id: replyTo ? replyTo.id : null });
      clearReply();
    }catch(e){
      const msg = String(e?.message || e || "");
      showNetworkError("Не удалось отправить сообщение.");
      addSystem(`❌ Сообщение не отправлено: ${msg || "повторите попытку."}`);
    }
  }

  // =========================
  // Read marker (unread + ✓✓ read)
  // =========================
  let lastMarked = 0;
  async function maybeMarkRead(){
    if (!token || !activeChatId) return;
    if (!lastMsgId) return;
    if (!isNearBottom($("msgs"))) return;
    if (lastMsgId <= lastMarked) return;

    lastMarked = lastMsgId;
    try{
      await api(`/api/chats/${encodeURIComponent(activeChatId)}/read?last_id=${lastMsgId}`, "POST");
      refreshChats(false).catch(()=>{});
    }catch(_){}
  }

  // =========================
  // Invite
  // =========================
  async function inviteUser(){
    if (!token) return openAuth("login");
    if (!activeChatId) return addSystem("⚠️ Сначала выбери чат.");
    if (activeChatType !== "group") return addSystem("⚠️ Приглашение доступно только в групповом чате.");

    const u = prompt("Логин пользователя для приглашения:");
    if (!u) return;

    try{
      await api(`/api/chats/${encodeURIComponent(activeChatId)}/invite`, "POST", { username: u.trim() });
      addSystem(`✅ Приглашен: ${u.trim()}`);
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  }

  function setReply(msg){
    replyTo = msg;
    const box = $("replyBox");
    box.classList.remove("is-hidden");
    box.textContent = `↪ reply to ${msg.sender}: ${(msg.text||"").slice(0,80)}`;
  }
  function clearReply(){
    replyTo = null;
    $("replyBox").classList.add("is-hidden");
    $("replyBox").textContent = "";
  }

  async function addReaction(messageId, emoji){
    try{
      await api(`/api/messages/${messageId}/reactions`, "POST", { emoji });
    }catch(e){ addSystem("❌ "+(e.message||e)); }
  }

  async function removeReaction(messageId, emoji){
    try{
      await api(`/api/messages/${messageId}/reactions?emoji=${encodeURIComponent(emoji)}`, "DELETE");
    }catch(e){ addSystem("❌ "+(e.message||e)); }
  }

  async function toggleReaction(messageId, emoji){
    const el = msgElById.get(Number(messageId));
    const mine = JSON.parse(el?.dataset.myReactions || "[]");
    if (mine.includes(emoji)) return removeReaction(messageId, emoji);
    return addReaction(messageId, emoji);
  }

  function applyReactionEvent(messageId, emoji, username, added){
    const el = msgElById.get(Number(messageId));
    if (!el) return;
    const holder = el.querySelector('[data-role="reactions"]');
    if (!holder) return;
    const mine = new Set(JSON.parse(el.dataset.myReactions || "[]"));
    if (username === me){
      if (added) mine.add(emoji);
      else mine.delete(emoji);
      el.dataset.myReactions = JSON.stringify(Array.from(mine));
    }

    const current = {};
    holder.querySelectorAll(".react[data-emoji]").forEach(btn => {
      const em = btn.dataset.emoji;
      const cnt = Number((btn.textContent || "").split(" ").pop() || 0);
      current[em] = cnt;
    });
    current[emoji] = Math.max(0, (current[emoji] || 0) + (added ? 1 : -1));
    if (!current[emoji]) delete current[emoji];
    renderReactions(holder, messageId, current, Array.from(mine));
  }

  function jumpToMessage(messageId){
    const target = msgElById.get(Number(messageId));
    if (!target) return addSystem("⚠️ Оригинал сообщения не найден");
    target.scrollIntoView({ behavior: "smooth", block: "center" });
    target.classList.add("flash");
    setTimeout(()=> target.classList.remove("flash"), 1400);
  }

  async function openMessageById(messageId){
    const id = Number(messageId);
    if (!id) return;
    if (!msgElById.has(id)){
      await loadHistory();
    }
    jumpToMessage(id);
  }

  async function saveProfile(){
    try{
      const display_name = $("profileDisplayName").value.trim();
      const bio = $("profileBio").value.trim();
      await api("/api/profile", "PATCH", { display_name, bio });
      displayName = display_name;
      profileBio = bio;
      localStorage.setItem("display_name", displayName);
      localStorage.setItem("profile_bio", profileBio);
      $("profileHint").textContent = "✅ Профиль сохранен";
    }catch(e){
      $("profileHint").textContent = "❌ " + (e.message || e);
    }
  }

  async function uploadStory(){
    const f = $("storyFile").files && $("storyFile").files[0];
    if (!f) return;
    const fd = new FormData();
    fd.append("file", f);
    fd.append("caption", $("storyCaption").value.trim());
    try{
      await request("/api/stories", { method: "POST", body: fd, auth: true });
      $("storyFile").value = "";
      $("storyCaption").value = "";
      $("profileHint").textContent = "✅ История опубликована";
      await loadStories();
    }catch(e){
      $("profileHint").textContent = "❌ " + (e.message || e);
    }
  }

  async function loadStories(){
    if (!token) return;
    const data = await api("/api/stories");
    stories = data.stories || [];
    renderStories();
    const mine = stories.filter(s => s.username === me);
    const box = $("myStories");
    box.innerHTML = "";
    if (!mine.length){
      box.textContent = "У тебя пока нет историй.";
      return;
    }
    for (const s of mine){
      const row = document.createElement("div");
      const t = document.createElement("span");
      t.textContent = `${new Date((s.created_at||0)*1000).toLocaleTimeString()} • ${s.caption || "без подписи"}`;
      const del = document.createElement("button");
      del.className = "btn btn--danger";
      del.style.marginLeft = "6px";
      del.textContent = "Удалить";
      del.onclick = async ()=>{
        await api(`/api/stories/${s.id}`, "DELETE");
        await loadStories();
      };
      row.appendChild(t);
      row.appendChild(del);
      box.appendChild(row);
    }
  }

  async function loadAvatarHistory(){
    if (!token) return;
    const data = await api('/api/avatar/history');
    avatarHistory = data.items || [];
    const box = $("myStories");
    const hist = document.createElement('div');
    hist.style.marginTop = '8px';
    if (!avatarHistory.length){
      hist.innerHTML = '<b>Предыдущие аватары:</b> нет';
      box.appendChild(hist);
      return;
    }
    hist.innerHTML = '<b>Предыдущие аватары:</b>';
    const list = document.createElement('div');
    list.className = 'overview-grid';
    for (const item of avatarHistory){
      const row = document.createElement('button');
      row.type = 'button';
      row.className = 'overview-item';
      row.innerHTML = `<div class="small">${new Date((item.created_at||0)*1000).toLocaleString()}</div>`;
      row.onclick = ()=>openMediaViewer(item.avatar_url, 'image', 'Аватар');
      const del = document.createElement('button');
      del.type = 'button';
      del.className = 'btn btn--danger';
      del.textContent = 'Удалить';
      del.onclick = async (e)=>{
        e.stopPropagation();
        await api(`/api/avatar/history/${item.id}`, 'DELETE');
        await loadStories();
        await loadAvatarHistory();
      };
      const wrap = document.createElement('div');
      wrap.className = 'row';
      wrap.appendChild(row);
      wrap.appendChild(del);
      list.appendChild(wrap);
    }
    hist.appendChild(list);
    box.appendChild(hist);
  }

  function renderStories(){
    const bar = $("storiesBar");
    if (!stories.length){ bar.innerHTML = ""; return; }
    const grouped = new Map();
    for (const s of stories){
      if (!grouped.has(s.username)) grouped.set(s.username, s);
    }
    bar.innerHTML = "";
    for (const [username, s] of grouped.entries()){
      const item = document.createElement("button");
      item.className = "story-item";
      const img = document.createElement("img");
      img.src = s.avatar_url || "";
      img.alt = username;
      const name = document.createElement("span");
      name.textContent = s.display_name || username;
      item.appendChild(img);
      item.appendChild(name);
      item.onclick = () => openUserProfile(username);
      bar.appendChild(item);
    }
  }

  async function togglePin(messageId){
    try{
      await api(`/api/chats/${encodeURIComponent(activeChatId)}/pins`, "POST", { message_id: Number(messageId) });
      await loadPins();
    }catch(e){ addSystem("❌ "+(e.message||e)); }
  }

  async function loadPins(){
    if (!activeChatId) return;
    try{
      const data = await api(`/api/chats/${encodeURIComponent(activeChatId)}/pins`);
      const pins = data.pins || [];
      const bar = $("pinsBar");
      if (!pins.length){ bar.classList.add("is-hidden"); bar.textContent = ""; return; }
      bar.classList.remove("is-hidden");
      bar.textContent = "📌 " + pins.slice(0,3).map(p => `${p.sender}: ${(p.text||"").slice(0,35)}`).join(" • ");
    }catch(_){ }
  }

  async function muteChat(){
    if (!activeChatId) return;
    const min = Number(prompt("Минут тихого режима (0 = выключить)", "60") || "0");
    if (Number.isNaN(min)) return;
    try{
      await api(`/api/chats/${encodeURIComponent(activeChatId)}/mute`, "POST", { muted_minutes: min });
      addSystem(min ? `🔕 muted ${min}m` : "🔔 unmuted");
      await refreshChats(false);
    }catch(e){ addSystem("❌ "+(e.message||e)); }
  }

  async function loadContacts(){
    const data = await api('/api/contacts');
    contacts = data.contacts || [];
    const list = $("contactsList");
    list.innerHTML = '';
    if (!contacts.length){
      list.innerHTML = '<div class="small">Контактов пока нет</div>';
      return;
    }
    for (const c of contacts){
      const row = document.createElement('div');
      row.className = 'chatitem';
      const left = document.createElement('div');
      left.className = 'left';
      const title = document.createElement('div');
      title.className = 'title';
      title.textContent = c.display_name || c.username;
      const sub = document.createElement('div');
      sub.className = 'sub';
      sub.textContent = `@${c.username} • ${c.online ? 'в сети' : 'не в сети'}`;
      left.appendChild(title);
      left.appendChild(sub);
      row.appendChild(left);

      const actions = document.createElement('div');
      actions.className = 'contact-actions';

      const profileBtn = document.createElement('button');
      profileBtn.type = 'button';
      profileBtn.className = 'btn btn--secondary contact-action-btn';
      profileBtn.textContent = 'Профиль';
      profileBtn.onclick = async ()=>{
        closeContacts();
        await openUserProfile(c.username);
      };

      const dmBtn = document.createElement('button');
      dmBtn.type = 'button';
      dmBtn.className = 'btn btn--secondary contact-action-btn';
      dmBtn.textContent = 'SMS';
      dmBtn.title = `Написать @${c.username}`;
      dmBtn.onclick = async ()=>{
        closeContacts();
        await createDM(c.username);
      };

      actions.appendChild(profileBtn);
      actions.appendChild(dmBtn);
      row.appendChild(actions);
      list.appendChild(row);
    }
  }

  function openContacts(){
    if (!token) return openAuth('login');
    modalManager.open("contactsOverlay");
    loadContacts().catch((e)=>addSystem('❌ '+(e.message||e)));
  }

  function closeContacts(){
    modalManager.close("contactsOverlay");
  }

  async function addContact(){
    const u = $("contactUsername").value.trim();
    if (!u) return;
    await api('/api/contacts', 'POST', { username: u });
    $("contactUsername").value = '';
    await loadContacts();
  }

  const chatInfoState = { activeTab: "info", data: null };

  function setChatInfoTab(tab){
    chatInfoState.activeTab = tab;
    document.querySelectorAll(".chat-info-tab").forEach((btn)=>{
      const active = btn.dataset.tab === tab;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-selected", active ? "true" : "false");
    });
    renderChatInfoTab();
  }

  function renderChatInfoTab(){
    const list = $("chatInfoTabList");
    const data = chatInfoState.data || {};
    const media = data.media || [];
    const links = data.links || [];
    const members = data.members || [];
    const messages = data.messages || [];

    const mediaOnly = media.filter((m)=> ["image", "video"].includes(String(m.media_kind || "").toLowerCase()));

    const map = {
      info: {
        title: "Info",
        items: [
          { label: "Найдено сообщений", value: messages.length },
          { label: "Медиа (фото/видео)", value: mediaOnly.length },
          { label: "Ссылки", value: links.length },
          { label: "Участники", value: members.length },
        ],
        empty: "Информация о чате недоступна",
      },
      media: { title: "Media", items: mediaOnly, empty: "В чате пока нет фото/видео" },
      members: { title: "Members", items: members, empty: "В чате пока нет участников" },
    };

    const current = map[chatInfoState.activeTab] || map.info;
    list.innerHTML = `<b>${current.title}:</b>`;

    if (!current.items.length){
      list.insertAdjacentHTML("beforeend", `<div class="small">${current.empty}</div>`);
      return;
    }

    const wrap = document.createElement("div");
    wrap.className = "overview-grid";

    for (const item of current.items){
      const card = document.createElement("button");
      card.type = "button";
      card.className = "overview-item";

      if (chatInfoState.activeTab === "info"){
        const titleWrap = document.createElement("div");
        const titleStrong = document.createElement("b");
        titleStrong.textContent = item.label || "Параметр";
        titleWrap.appendChild(titleStrong);
        const meta = document.createElement("div");
        meta.className = "small";
        meta.textContent = String(item.value ?? 0);
        card.appendChild(titleWrap);
        card.appendChild(meta);
        card.onclick = () => {};
      } else if (chatInfoState.activeTab === "members"){
        const titleWrap = document.createElement("div");
        const titleStrong = document.createElement("b");
        titleStrong.textContent = item.display_name || item.username || "Пользователь";
        titleWrap.appendChild(titleStrong);
        const meta = document.createElement("div");
        meta.className = "small";
        meta.textContent = `@${item.username || ""} • ${item.online ? "в сети" : "не в сети"}`;
        card.appendChild(titleWrap);
        card.appendChild(meta);
        card.onclick = () => openUserProfile(item.username);
      } else {
        const title = item.media_name || item.media_kind || "Файл";
        const titleWrap = document.createElement("div");
        const titleStrong = document.createElement("b");
        titleStrong.textContent = title;
        titleWrap.appendChild(titleStrong);
        const meta = document.createElement("div");
        meta.className = "small";
        meta.textContent = `${item.sender || ''} • ${fmtTs(item.created_at)}`;
        card.appendChild(titleWrap);
        card.appendChild(meta);
        card.onclick = async () => {
          closeChatInfo();
          await openMessageById(item.id);
        };
      }

      wrap.appendChild(card);
    }
    list.appendChild(wrap);
  }

  async function openChatInfo(){
    if (!activeChatId) return;
    modalManager.open("chatInfoOverlay");
    $("chatInfoTitle").textContent = activeChatTitle;
    setChatInfoTab("info");
    await loadChatOverview('');
  }

  function closeChatInfo(){
    modalManager.close("chatInfoOverlay");
  }

  async function loadChatOverview(keyword){
    const data = await api(`/api/chats/${encodeURIComponent(activeChatId)}/overview?q=${encodeURIComponent(keyword||'')}`);
    const msgs = data.messages || [];
    chatInfoState.data = data;

    const resultsBox = $("chatInfoResults");
    resultsBox.textContent = "";
    const resultsTitle = document.createElement("b");
    resultsTitle.textContent = "Сообщения:";
    resultsBox.appendChild(resultsTitle);
    if (!msgs.length){
      const empty = document.createElement("div");
      empty.className = "small";
      empty.textContent = "нет";
      resultsBox.appendChild(empty);
    } else {
      for (const m of msgs){
        const row = document.createElement("div");
        row.className = "small";
        row.textContent = `${m.sender || ""}: ${String(m.text || "").slice(0,80)}`;
        resultsBox.appendChild(row);
      }
    }

    renderChatInfoTab();
  }


  // =========================
  // Context menu
  // =========================
  const ctx = {
    el: $("ctxMenu"),
    reply: $("ctxReply"),
    pin: $("ctxPin"),
    forward: $("ctxForward"),
    edit: $("ctxEdit"),
    delMe: $("ctxDeleteMe"),
    delAll: $("ctxDeleteAll"),
    cancel: $("ctxCancel"),
    meta: $("ctxMeta"),
    msgEl: null,
    msgId: 0,
    sender: ""
  };

  function closeCtx(){
    ctx.el.classList.remove("open");
    ctx.msgEl = null;
    ctx.msgId = 0;
    ctx.sender = "";
    if (ctx.meta) ctx.meta.innerHTML = "Доставка: —<br/>Прочитано: —";
  }

  function openCtxForMsg(msgEl, x, y){
    const msgId = Number(msgEl.dataset.msgId || "0");
    const sender = msgEl.dataset.sender || "";
    const deleted = (msgEl.dataset.deletedForAll === "true");
    if (!msgId) return;

    ctx.msgEl = msgEl;
    ctx.msgId = msgId;
    ctx.sender = sender;

    ctx.edit.style.display = (!deleted && sender === me) ? "" : "none";
    ctx.delAll.style.display = (!deleted && sender === me) ? "" : "none";
    ctx.delMe.style.display = "";

    if (ctx.meta) {
      ctx.meta.innerHTML = "Доставка: ⏳ загружаем...<br/>Прочитано: ⏳ загружаем...";
      loadCtxMessageStatus(msgId);
    }

    ctx.el.style.left = `${Math.min(x, window.innerWidth - 240)}px`;
    ctx.el.style.top = `${Math.min(y, window.innerHeight - 200)}px`;
    ctx.el.classList.add("open");
  }

  async function forwardMessage(msgId){
    const targets = (chats || []).filter((c)=> String(c.id || "") !== String(activeChatId || ""));
    if (!targets.length){
      addSystem("⚠️ Нет доступных чатов для пересылки.");
      return;
    }

    const listText = targets
      .map((c, i)=> `${i+1}. ${c.title || c.id} (${c.type || "chat"})`)
      .join("\n");
    const input = prompt(`Переслать сообщение в:
${listText}

Введите номер чата:`);
    if (input == null) return;
    const idx = Number(input.trim());
    if (!Number.isInteger(idx) || idx < 1 || idx > targets.length){
      addSystem("⚠️ Неверный номер чата.");
      return;
    }

    const target = targets[idx - 1];
    try{
      await api(`/api/messages/${msgId}/forward`, "POST", { target_chat_id: target.id });
      addSystem(`✅ Сообщение переслано в: ${target.title || target.id}`);
      refreshChats(false).catch(()=>{});
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  }

  async function loadCtxMessageStatus(messageId){
    try{
      const data = await api(`/api/messages/${messageId}/status`);
      if (!ctx.meta || ctx.msgId !== Number(messageId || 0)) return;

      const membersTotal = Number(data.members_total || 0);
      const deliveredCount = Number(data.delivered_count || 0);
      const readCount = Number(data.read_count || 0);
      const deliveredLatest = data.delivered_latest ? fmtTs(data.delivered_latest) : "—";
      const readLatest = data.read_latest ? fmtTs(data.read_latest) : "—";

      const deliveredLine = membersTotal > 1
        ? `Доставка: ${deliveredCount}/${membersTotal} (посл.: ${deliveredLatest})`
        : `Доставка: ${deliveredLatest}`;
      const readLine = membersTotal > 1
        ? `Прочитано: ${readCount}/${membersTotal} (посл.: ${readLatest})`
        : `Прочитано: ${readLatest}`;

      ctx.meta.innerHTML = `${deliveredLine}<br/>${readLine}`;
    }catch(_){
      if (!ctx.meta || ctx.msgId !== Number(messageId || 0)) return;
      ctx.meta.innerHTML = "Доставка: —<br/>Прочитано: —";
    }
  }

  async function startInlineEdit(msgEl){
    closeCtx();
    const msgId = Number(msgEl.dataset.msgId || "0");
    if (!msgId) return;

    const body = msgEl.querySelector('[data-role="body"]');
    if (!body) return;

    const old = body.textContent || "";
    const input = document.createElement("input");
    input.value = old;
    input.style.marginTop = "6px";

    const row = document.createElement("div");
    row.style.display = "flex";
    row.style.gap = "8px";
    row.style.marginTop = "8px";

    const save = document.createElement("button");
    save.className = "btn btn--secondary";
    save.textContent = "Save";
    save.style.flex = "1";

    const cancel = document.createElement("button");
    cancel.className = "btn btn--danger";
    cancel.textContent = "Отмена";
    cancel.style.flex = "1";

    const original = old;
    body.textContent = "";
    body.appendChild(input);
    body.appendChild(row);
    row.appendChild(save);
    row.appendChild(cancel);

    const cleanup = () => { body.innerHTML = ""; body.textContent = original; };

    cancel.onclick = () => cleanup();
    save.onclick = async () => {
      const newText = input.value.trim();
      if (newText.length > 2000) return addSystem("⚠️ Слишком длинно (макс 2000).");
      try{
        await api(`/api/messages/${msgId}`, "PATCH", { text: newText });
        applyEdited(msgId, newText, true);
        body.innerHTML = "";
        body.textContent = newText;
      }catch(e){
        addSystem("❌ " + (e.message || e));
        cleanup();
      }
    };

    input.addEventListener("keydown", (e)=>{
      if (e.key === "Enter") save.click();
      if (e.key === "Escape") cancel.click();
    });
    setTimeout(()=> input.focus(), 10);
  }

  async function deleteForMe(msgId){
    try{
      await api(`/api/messages/${msgId}?scope=me`, "DELETE");
      const el = msgElById.get(msgId);
      if (el) {
        const row = el.closest(".msg-row");
        if (row) row.remove();
        else el.remove();
      }
      msgElById.delete(msgId);
      refreshChats(false).catch(()=>{});
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  }

  async function deleteForAll(msgId){
    try{
      await api(`/api/messages/${msgId}?scope=all`, "DELETE");
      applyDeletedAll(msgId);
      refreshChats(false).catch(()=>{});
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  }

  ctx.reply.onclick = () => { if (ctx.msgEl) { setReply({ id: ctx.msgId, sender: ctx.sender, text: (ctx.msgEl.querySelector('[data-role="body"]')?.textContent||"") }); closeCtx(); } };
  ctx.pin.onclick = () => { const id = ctx.msgId; closeCtx(); if (id) togglePin(id); };
  ctx.forward.onclick = () => { const id = ctx.msgId; closeCtx(); if (id) forwardMessage(id); };
  ctx.edit.onclick = () => { if (ctx.msgEl) startInlineEdit(ctx.msgEl); };
  ctx.delMe.onclick = () => { const id = ctx.msgId; closeCtx(); if (id) deleteForMe(id); };
  ctx.delAll.onclick = () => { const id = ctx.msgId; closeCtx(); if (id) deleteForAll(id); };
  ctx.cancel.onclick = () => closeCtx();
  document.addEventListener("click", (e)=>{ if (!ctx.el.contains(e.target)) closeCtx(); });

  // =========================
  // Session UI
  // =========================
  function setWhoami(){
    $("whoText").textContent = token ? `@${me}` : "Не авторизован";
    toggleHidden($("btnOpenAuth"), !!token);
    if (!token) closeProfileMenu();

    const img = $("topAvatar");
    if (token && avatarUrl){
      img.src = avatarUrl;
      img.classList.add("show");
    } else {
      img.classList.remove("show");
      img.removeAttribute("src");
    }
  }

  async function clearClientSessionData(){
    if ("caches" in window){
      try{
        const keys = await caches.keys();
        await Promise.all(keys.map((key) => caches.delete(key)));
      }catch(_){ }
    }

    if (!("indexedDB" in window) || typeof indexedDB.databases !== "function") return;
    try{
      const dbs = await indexedDB.databases();
      await Promise.all(
        dbs
          .map((db) => String(db?.name || ""))
          .filter(Boolean)
          .map((dbName) => new Promise((resolve) => {
            const req = indexedDB.deleteDatabase(dbName);
            req.onsuccess = () => resolve();
            req.onerror = () => resolve();
            req.onblocked = () => resolve();
          }))
      );
    }catch(_){ }
  }

  async function logout(){
    stopAllExcept(null);
    endCall({ silent:true });
    const lastMessageStorageKey = getLastMessageStorageKey();
    token = ""; me = ""; avatarUrl = "";
    displayName = "";
    profileBio = "";
    localStorage.removeItem("token");
    localStorage.removeItem("username");
    localStorage.removeItem("avatar_url");
    localStorage.removeItem("display_name");
    localStorage.removeItem("profile_bio");
    localStorage.removeItem("activeChatId");
    localStorage.removeItem(lastMessageStorageKey);

    try{ await request("/api/logout", { method: "POST", auth: false, retry: false }); }catch(_){ }
    await clearClientSessionData();

    if (ws) { try{ ws.close(); }catch{} ws = null; }

    $("msgs").innerHTML = "";
    chats = [];
    activeChatId = "";
    activeChatTitle = "";
    activeChatType = "";
    activeChatCreatedBy = "";
    renderChatList();
    setWhoami();
    setStatus("👋 Logged out");
    setNet("не в сети");
    closeSidebar();
    closeSidebarMoreMenu();
    closeSheet();
    closeProfile();
    closeHelp();
    closeVoicePreview();
    openAuth("login");
  }

  // =========================
  // Wiring
  // =========================
  modalManager.register({ overlayId: "authOverlay", panelSelector: ".modal", close: closeAuth, ariaLabel: "Авторизация" });
  modalManager.register({ overlayId: "sheetOverlay", panelSelector: ".sheet", close: closeSheet, ariaLabel: "Создание чата" });
  modalManager.register({ overlayId: "profileOverlay", panelSelector: ".sheet", close: closeProfile, ariaLabel: "Профиль" });
  modalManager.register({ overlayId: "contactsOverlay", panelSelector: ".sheet", close: closeContacts, ariaLabel: "Контакты" });
  modalManager.register({ overlayId: "chatInfoOverlay", panelSelector: ".sheet", close: closeChatInfo, ariaLabel: "Информация о чате" });
  modalManager.register({ overlayId: "helpOverlay", panelSelector: ".sheet", close: closeHelp, ariaLabel: "Справка" });
  modalManager.register({ overlayId: "userProfileOverlay", panelSelector: ".sheet", close: closeUserProfile, ariaLabel: "Профиль пользователя" });
  modalManager.register({ overlayId: "mediaViewerOverlay", panelSelector: ".modal", close: closeMediaViewer, ariaLabel: "Просмотр медиа" });
  modalManager.register({ overlayId: "voicePreviewOverlay", panelSelector: ".modal", close: () => { closeVoicePreview(); clearPreview(); }, ariaLabel: "Предпросмотр голосового сообщения" });
  modalManager.register({ overlayId: "callOverlay", panelSelector: ".modal", close: () => endCall({ silent:true }), ariaLabel: "Звонок" });
  $("btnToggleSidebar").onclick = () => toggleSidebar();
  $("drawerBackdrop").onclick = () => closeSidebar();
  window.addEventListener("resize", ()=> {
    syncSidebarTopOffset();
    if (!isMobile()) closeSidebar({ restoreFocus:false });
  });
  window.addEventListener("orientationchange", syncSidebarTopOffset);
  window.addEventListener("online", () => {
    setOfflineMode(false);
    if (token && (!ws || ws.readyState !== 1)) connectWS_GLOBAL();
  });
  window.addEventListener("offline", () => {
    setOfflineMode(true);
  });

  $("btnThemeToggle").onclick = () => toggleTheme();
  $("btnOpenAuth").onclick = () => openAuth("login");
  $("whoami").onclick = () => toggleProfileMenu();
  $("btnMenuMyProfile").onclick = () => { closeProfileMenu(); openProfile(); };
  $("btnMenuContacts").onclick = () => { closeProfileMenu(); openContacts(); };
  $("btnMenuHelp").onclick = () => { closeProfileMenu(); openHelp(); };
  $("btnMenuLogout").onclick = () => { closeProfileMenu(); logout(); };
  $("btnContacts").onclick = () => { closeSidebarMoreMenu(); openContacts(); };
  $("btnHelp").onclick = () => { closeSidebarMoreMenu(); openHelp(); };
  $("btnChatInfo").onclick = () => openChatInfo();
  $("btnChatInfo").onkeydown = (e) => {
    if (e.key === "Enter" || e.key === " "){
      e.preventDefault();
      openChatInfo();
    }
  };
  $("btnChatActions").onclick = (e) => {
    e.stopPropagation();
    toggleChatActionsMenu();
  };
  $("btnChatActionInfo").onclick = () => {
    closeChatActionsMenu();
    openChatInfo().then(()=> setChatInfoTab("info"));
  };
  $("btnChatActionMedia").onclick = () => {
    closeChatActionsMenu();
    openChatInfo().then(()=> setChatInfoTab("media"));
  };
  $("btnChatActionMembers").onclick = () => {
    closeChatActionsMenu();
    openChatInfo().then(()=> setChatInfoTab("members"));
  };
  $("btnChatActionVoiceCall").onclick = () => {
    closeChatActionsMenu();
    startCall("voice");
  };
  $("btnChatActionVideoCall").onclick = () => {
    closeChatActionsMenu();
    startCall("video");
  };
  $("btnToggleCallMic").onclick = () => toggleCallMic();
  $("btnToggleCallCamera").onclick = () => toggleCallCamera();
  $("btnEndCall").onclick = () => endCall();
  $("btnCloseCall").onclick = () => endCall({ silent:true });

  $("tabLogin").onclick = () => setAuthTab("login");
  $("tabRegister").onclick = () => setAuthTab("register");
  $("btnCloseAuth").onclick = () => { if (!token) return; closeAuth(); };
  $("btnAuthSubmit").onclick = () => authSubmit();
  $("authPassword").addEventListener("keydown", (e)=>{ if (e.key === "Enter") authSubmit(); });

  $("btnOpenCreateGroup").onclick = () => openSheet("group");
  $("btnSidebarMore").onclick = () => toggleSidebarMoreMenu();
  $("btnCloseSheet").onclick = () => closeSheet();
  $("btnSheetCancel").onclick = () => closeSheet();
  $("btnSheetOk").onclick = async () => {
    try{
      const value = sheet.input.value;
      const mode = sheet.mode;
      closeSheet();
      if (mode === "group") await createGroupChat(value);
      if (mode === "dm") await createDM(value);
      if (isMobile()) closeSidebar();
    }catch(e){
      addSystem("❌ " + (e.message || e));
    }
  };
  $("sheetInput").addEventListener("keydown", (e)=>{ if (e.key === "Enter") $("btnSheetOk").click(); });

  $("btnCloseProfile").onclick = () => closeProfile();
  $("btnCloseHelp").onclick = () => closeHelp();
  $("btnCloseUserProfile").onclick = () => closeUserProfile();
  $("btnCloseContacts").onclick = () => closeContacts();
  $("btnCloseChatInfo").onclick = () => closeChatInfo();
  $("btnAddContact").onclick = () => addContact().catch(e=> addSystem("❌ " + (e.message || e)));
  $("btnChatInfoSearch").onclick = () => loadChatOverview($("chatInfoSearch").value.trim()).catch(e=> addSystem("❌ " + (e.message || e)));
  document.querySelectorAll(".chat-info-tab").forEach((btn)=>{
    btn.onclick = () => setChatInfoTab(btn.dataset.tab || "media");
  });

  document.addEventListener("click", (e) => {
    const menu = $("chatActionsMenu");
    const toggle = $("btnChatActions");
    if (!chatActionsMenuOpen || !menu || !toggle) return;
    if (menu.contains(e.target) || toggle.contains(e.target)) return;
    closeChatActionsMenu();
  });
  document.addEventListener("click", (e)=>{
    const menu = $("profileMenu");
    const trigger = $("whoami");
    if (!menu.classList.contains("open")) return;
    if (menu.contains(e.target) || trigger.contains(e.target)) return;
    closeProfileMenu();
  });
  $("btnUploadAvatar").onclick = () => uploadAvatar();
  $("btnSaveProfile").onclick = () => saveProfile();
  $("btnUploadStory").onclick = () => uploadStory();

  $("btnCloseMediaViewer").onclick = () => closeMediaViewer();

  $("btnCloseVoicePreview").onclick = () => { closeVoicePreview(); clearPreview(); };
  $("btnCancelVoiceNow").onclick = () => { closeVoicePreview(); clearPreview(); };
  $("btnSendVoiceNow").onclick = () => sendVoiceNow();

  $("btnAttach").onclick = () => $("file").click();
  $("file").addEventListener("change", async () => {
    const f = $("file").files && $("file").files[0];
    if (!f) return;
    try{ await uploadMedia(f); }
    catch(e){ addSystem("❌ " + (e.message || e)); }
  });

  $("btnSend").onclick = () => send();
  $("text").addEventListener("keydown", (e)=>{ if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } if (e.key === "Escape") clearReply(); });
  $("text").addEventListener("input", ()=> onLocalTyping());

  recBtn.addEventListener("pointerdown", async (e)=>{ e.preventDefault(); if (!rec.active) await startHoldRec(); });
  recBtn.addEventListener("pointerup", (e)=>{ e.preventDefault(); stopHoldRec(); });
  recBtn.addEventListener("pointercancel", (e)=>{ e.preventDefault(); stopHoldRec(); });
  recBtn.addEventListener("pointerleave", (e)=>{ if (rec.active) stopHoldRec(); });

  $("btnRefreshChats").onclick = () => { closeSidebarMoreMenu(); refreshChats(false).catch(e=> addSystem("❌ " + e.message)); };
  $("btnLoadHistory").onclick = () => { closeSidebarMoreMenu(); loadHistory().catch(e=> addSystem("❌ " + e.message)); };
  $("btnInvite").onclick = () => { closeSidebarMoreMenu(); inviteUser(); };
  $("btnMute").onclick = () => { closeSidebarMoreMenu(); muteChat(); };
  $("chatSearch").addEventListener("input", ()=> renderChatList());
  document.querySelectorAll(".chat-filter").forEach((btn)=>{
    btn.addEventListener("click", ()=>{
      activeChatFilter = btn.dataset.filter || "all";
      document.querySelectorAll(".chat-filter").forEach((el)=> el.classList.toggle("is-active", el === btn));
      renderChatList();
    });
  });


  document.addEventListener("click", (e)=>{
    const menu = $("sidebarMoreMenu");
    const trigger = $("btnSidebarMore");
    if (!sidebarMoreMenuOpen || !menu || !trigger) return;
    if (menu.contains(e.target) || trigger.contains(e.target)) return;
    closeSidebarMoreMenu();
  });

  $("msgs").addEventListener("scroll", ()=> {
    updateToBottom();
    const box = $("msgs");
    if (box.scrollTop <= 40){
      loadOlderMessages().catch(()=>{});
    }
  }, { passive:true });
  toBottomBtn.onclick = () => { scrollToBottom($("msgs")); updateToBottom(); };

  document.addEventListener("keydown", (e)=>{
    if (e.key === "Escape"){
      if (modalManager.closeTop()) return;
      closeCtx();
      closeSidebar();
      closeSidebarMoreMenu();
      closeProfileMenu();
    }
    if (e.key === "Tab" && sidebar.classList.contains("open")){
      const focusable = Array.from(sidebar.querySelectorAll('button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'))
        .filter((el)=> !el.disabled && el.offsetParent !== null);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (e.shiftKey && document.activeElement === first){
        e.preventDefault();
        last.focus();
      } else if (!e.shiftKey && document.activeElement === last){
        e.preventDefault();
        first.focus();
      }
    }
  });

  // =========================
  // Bootstrap
  // =========================
  syncSidebarTopOffset();
  initTheme();
  initMobileAppBridge();
  registerServiceWorker();
  updateChatActionState();
  setWhoami();
  requestNotificationPermissionIfNeeded();
  setOfflineMode(!navigator.onLine);

  if (token){
    addSystem("🔁 Проверяем сессию…");
    api("/api/me")
      .then(async data => {
        me = data.username;
        avatarUrl = data.avatar_url || "";
        displayName = data.display_name || "";
        profileBio = data.bio || "";
        localStorage.setItem("username", me);
        localStorage.setItem("avatar_url", avatarUrl || "");
        localStorage.setItem("display_name", displayName || "");
        localStorage.setItem("profile_bio", profileBio || "");
        setWhoami();
        connectWS_GLOBAL();
        await refreshChats(true);
        loadStories().catch(()=>{});
        maybeShowHelpOnboarding();
      })
      .catch(() => {
        token = "";
        localStorage.removeItem("token");
        localStorage.removeItem("username");
        localStorage.removeItem("avatar_url");
        localStorage.removeItem("activeChatId");
        me = "";
        avatarUrl = "";
        setWhoami();
        addSystem("🔐 Сессия истекла. Войди снова.");
        openAuth("login");
      });
  } else {
    addSystem("🔐 Требуется авторизация.");
    openAuth("login");
    maybeShowHelpOnboarding();
  }
