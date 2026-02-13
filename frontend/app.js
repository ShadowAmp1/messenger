  const $ = (id) => document.getElementById(id);
  const toggleHidden = (el, hidden) => el.classList.toggle("is-hidden", !!hidden);
  const escapeHtml = (v) => String(v || "").replace(/[&<>"']/g, (ch)=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[ch]));
  let lastSidebarFocus = null;

  let token = localStorage.getItem("token") || "";
  let refreshToken = localStorage.getItem("refresh_token") || "";
  let me = localStorage.getItem("username") || "";
  let avatarUrl = localStorage.getItem("avatar_url") || "";
  let replyTo = null;
  let displayName = localStorage.getItem("display_name") || "";
  let profileBio = localStorage.getItem("profile_bio") || "";
  let stories = [];
  let avatarHistory = [];
  let contacts = [];

  const REACTION_EMOJIS = ["👍","❤️","😂","😮","🔥","🎉","👏","🤝","🙏","😢","😡","💯"];

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

  // GLOBAL WS
  let ws = null;

  let chats = [];
  let activeChatId = localStorage.getItem("activeChatId") || "";
  let activeChatTitle = "";
  let activeChatType = "";
  let activeChatCreatedBy = "";

  const msgElById = new Map();
  let lastMsgId = 0;

  function isMobile(){ return window.matchMedia("(max-width: 900px)").matches; }

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

  function addSystem(text){
    const box = $("msgs");
    const div = document.createElement("div");
    div.className = "msg";
    div.innerHTML = `
      <div class="meta"><b>System</b><span>—</span></div>
      <div>${String(text||"")}</div>
    `;
    box.appendChild(div);
    scrollToBottom(box);
    updateToBottom();
  }

  function fmtTs(ts){
    try{
      const d = new Date((ts||0)*1000);
      return d.toLocaleString(undefined, {hour:"2-digit", minute:"2-digit"});
    }catch{ return "—"; }
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

  async function tryRefresh(){
    if (!refreshToken) return false;
    const res = await fetch("/api/refresh", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: refreshToken })
    });
    if (!res.ok) return false;
    const data = await res.json();
    token = data.token || "";
    refreshToken = data.refresh_token || "";
    localStorage.setItem("token", token);
    localStorage.setItem("refresh_token", refreshToken);
    return !!token;
  }

  async function api(path, method="GET", body=null, retry=true){
    const opts = { method, headers: {} };
    if (token) opts.headers["Authorization"] = `Bearer ${token}`;
    if (body){
      opts.headers["Content-Type"] = "application/json";
      opts.body = JSON.stringify(body);
    }
    const res = await fetch(path, opts);
    const raw = await res.text();
    let data = {};
    try { data = JSON.parse(raw); } catch { data = { detail: raw }; }
    if (res.status === 401 && retry && await tryRefresh()){
      return api(path, method, body, false);
    }
    if (!res.ok) throw new Error((data && data.detail) ? String(data.detail) : `${res.status} ${res.statusText}`);
    return data;
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
    $("authOverlay").classList.add("open");
    $("authOverlay").setAttribute("aria-hidden","false");
    $("authUsername").value = me || $("authUsername").value || "";
    $("authPassword").value = "";
    setTimeout(()=> $("authUsername").focus(), 50);
  }
  function closeAuth(){
    $("authOverlay").classList.remove("open");
    $("authOverlay").setAttribute("aria-hidden","true");
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
    $("btnAuthSubmit").textContent = (authMode==="login") ? "Входим…" : "Регистрируем…";
    try{
      const data = await api(authMode==="login" ? "/api/login" : "/api/register", "POST", {username, password});
      token = data.token;
      refreshToken = data.refresh_token || "";
      me = data.username;
      localStorage.setItem("token", token);
      localStorage.setItem("refresh_token", refreshToken);
      localStorage.setItem("username", me);

      await refreshMe();
      setWhoami();
      requestNotificationPermissionIfNeeded();
      closeAuth();
      addSystem(`✅ Вход выполнен: ${me}`);

      connectWS_GLOBAL();
      await refreshChats(true);
      loadStories().catch(()=>{});
    }catch(e){
      showAuthError(String(e.message || e));
    }finally{
      authBusy = false;
      $("btnAuthSubmit").disabled = false;
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
    $("typing").textContent = names.length ? `${names.join(", ")} печатает…` : "";
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
    const url = `${proto}//${location.host}/ws/user?token=${encodeURIComponent(token)}`;

    setNet("connecting…");
    ws = new WebSocket(url);

    ws.onopen = () => setNet("online");
    ws.onclose = () => setNet("не в сети");
    ws.onerror = () => setNet("не в сети");

    ws.onmessage = (ev) => {
      let data = null;
      try{ data = JSON.parse(ev.data); }catch{ return; }

      // invited -> refresh chats
      if (data.type === "invited"){
        refreshChats(true).catch(()=>{});
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

      // edited
      if (data.type === "message_edited"){
        if (data.chat_id === activeChatId){
          applyEdited(data.id, data.text, true);
        }
        return;
      }

      // deleted for all
      if (data.type === "message_deleted_all"){
        if (data.chat_id === activeChatId){
          applyDeletedAll(data.id);
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
    sheet.overlay.classList.add("open");
    sheet.overlay.setAttribute("aria-hidden","false");
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
    sheet.overlay.classList.remove("open");
    sheet.overlay.setAttribute("aria-hidden","true");
    sheet.mode = null;
  }

  const profile = { overlay: $("profileOverlay") };
  function openProfile(){
    if (!token) return openAuth("login");
    $("profileHint").textContent = `@${me}`;
    $("profileDisplayName").value = displayName || "";
    $("profileBio").value = profileBio || "";
    profile.overlay.classList.add("open");
    profile.overlay.setAttribute("aria-hidden","false");
    loadStories().catch(()=>{});
    loadAvatarHistory().catch(()=>{});
  }
  function closeProfile(){
    profile.overlay.classList.remove("open");
    profile.overlay.setAttribute("aria-hidden","true");
  }

  // =========================
  // Voice hold-to-record + preview
  // =========================
  const recBtn = $("btnRecHold");
  let rec = { active:false, mr:null, chunks:[], stream:null };
  let preview = { blob:null, file:null, url:"" };

  function openVoicePreview(){
    $("voicePreviewOverlay").classList.add("open");
    $("voicePreviewOverlay").setAttribute("aria-hidden","false");
    const holder = $("voicePreviewPlayer");
    holder.innerHTML = "";
    const player = createVoicePlayer(preview.url);
    holder.appendChild(player.root);
    $("voicePreviewHint").textContent = "Прослушай и отправь / отмена.";
  }
  function closeVoicePreview(){
    $("voicePreviewOverlay").classList.remove("open");
    $("voicePreviewOverlay").setAttribute("aria-hidden","true");
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
    const res = await fetch("/api/upload", { method: "POST", headers: { "Authorization": `Bearer ${token}` }, body: fd });
    const raw = await res.text();
    let data = {};
    try { data = JSON.parse(raw); } catch { data = { detail: raw }; }
    if (!res.ok){ setStatus(""); throw new Error(data.detail || raw); }
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
      const res = await fetch("/api/avatar", {
        method: "POST",
        headers: { "Authorization": `Bearer ${token}` },
        body: fd
      });
      const raw = await res.text();
      let data = {};
      try { data = JSON.parse(raw); } catch { data = { detail: raw }; }
      if (!res.ok) throw new Error(data.detail || raw);
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
    if (c.id === "general") return false;
    if (c.type === "dm") return true;
    return (c.created_by === me);
  }

  function renderChatList(){
    const list = $("chatlist");
    list.innerHTML = "";
    if (!chats.length){
      const div = document.createElement("div");
      div.className = "small";
      div.textContent = "Нет чатов. Создай ➕ или 💬";
      list.appendChild(div);
      return;
    }

    for (const c of chats){
      const item = document.createElement("div");
      item.className = "chatitem" + (c.id === activeChatId ? " active" : "");

      const left = document.createElement("div");
      left.className = "left";

      const title = computeChatTitle(c);
      const t1 = document.createElement("div");
      t1.className = "title";
      t1.textContent = title.replace(/^ЛС:\s*/, "");

      const t2 = document.createElement("div");
      t2.className = "sub";
      if (c.last_text){
        const sender = c.last_sender === me ? "Ты" : c.last_sender;
        t2.textContent = `${sender}: ${String(c.last_text).slice(0,44)}`;
      } else {
        t2.textContent = "Нет сообщений";
      }

      left.appendChild(t1);
      left.appendChild(t2);

      const right = document.createElement("div");
      right.style.display = "flex";
      right.style.gap = "8px";
      right.style.alignItems = "center";

      if (c.unread && Number(c.unread) > 0 && c.id !== activeChatId){
        const u = document.createElement("span");
        u.className = "unread";
        u.textContent = String(c.unread);
        right.appendChild(u);
      }

      if (isChatMuted(c.id)){
        const m = document.createElement("span");
        m.className = "badge";
        m.textContent = "🔕";
        right.appendChild(m);
      }

      if (canDeleteChat(c)){
        const del = document.createElement("div");
        del.className = "trash";
        del.textContent = "🗑";
        del.title = "Удалить чат";
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
        right.appendChild(del);
      }

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
  }

  function selectChat(chatId){
    stopAllExcept(null);
    clearReply();
    msgElById.clear();
    lastMsgId = 0;

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
  function addMsg(m){
    const box = $("msgs");
    const row = document.createElement("div");
    row.className = "msg-row" + ((m.sender === me) ? " me" : "");

    const div = document.createElement("div");
    div.className = "msg" + ((m.sender === me) ? " me" : "");
    div.dataset.msgId = String(m.id || "");
    div.dataset.sender = String(m.sender || "");
    div.dataset.deletedForAll = String(!!m.deleted_for_all);
    div.dataset.myReactions = JSON.stringify(m.my_reactions || []);

    const meta = document.createElement("div");
    meta.className = "meta";

    const left = document.createElement("b");
    left.textContent = m.sender || "—";
    const right = document.createElement("span");
    right.textContent = fmtTs(m.created_at);

    if (m.is_edited){
      const ed = document.createElement("span");
      ed.className = "edited";
      ed.textContent = "edited";
      right.appendChild(ed);
    }

    meta.appendChild(left);
    meta.appendChild(right);
    div.appendChild(meta);

    const body = document.createElement("div");
    body.dataset.role = "body";

    if (m.deleted_for_all){
      body.className = "deleted";
      body.textContent = "Сообщение удалено";
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
        rep.textContent = `↪ ${m.reply_sender || "user"}: ${t ? t.slice(0,80) : "[media]"}`;
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
        img.onclick = () => window.open(media_url, "_blank");
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

      body.textContent = text;
      body.style.marginTop = (media_url ? "8px" : "0");
      div.appendChild(body);
    }

    // ticks for my messages
    if (m.sender === me){
      const st = document.createElement("div");
      st.className = "ticks";
      st.dataset.role = "status";
      st.textContent = "✓"; // sent
      div.appendChild(st);
    }

    const reacts = document.createElement("div");
    reacts.className = "reactions";
    reacts.dataset.role = "reactions";
    renderReactions(reacts, m.id, m.reactions || {}, m.my_reactions || []);

    const addBtn = document.createElement("button");
    addBtn.className = "react add-react";
    addBtn.textContent = "➕";
    addBtn.onclick = (e) => {
      e.stopPropagation();
      openEmojiPicker(m.id, addBtn);
    };
    reacts.appendChild(addBtn);
    div.appendChild(reacts);

    // context menu
    div.addEventListener("contextmenu", (e)=>{
      e.preventDefault();
      openCtxForMsg(div, e.clientX, e.clientY);
    });

    let lpTimer = null;
    div.addEventListener("pointerdown", (e)=>{
      if (e.pointerType === "mouse") return;
      lpTimer = setTimeout(()=> openCtxForMsg(div, e.clientX, e.clientY), 520);
    });
    div.addEventListener("pointerup", ()=> { if (lpTimer) clearTimeout(lpTimer); lpTimer = null; });
    div.addEventListener("pointercancel", ()=> { if (lpTimer) clearTimeout(lpTimer); lpTimer = null; });

    msgElById.set(m.id, div);
    lastMsgId = Math.max(lastMsgId, Number(m.id||0));

    const avatarNode = createMessageAvatar(m.sender, m.sender === me, m.sender_avatar_url);
    if (m.sender === me){
      row.appendChild(div);
      row.appendChild(avatarNode);
    } else {
      row.appendChild(avatarNode);
      row.appendChild(div);
    }

    const stick = isNearBottom(box);
    box.appendChild(row);
    if (stick) {
      scrollToBottom(box);
      maybeMarkRead();
    }
    updateToBottom();
  }

  function renderReactions(holder, messageId, reactionMap, myReactions){
    holder.querySelectorAll(".react[data-emoji]").forEach(n => n.remove());
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
      holder.insertBefore(rb, holder.querySelector(".add-react"));
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

  function applyEdited(id, text, isEdited){
    const el = msgElById.get(id);
    if (!el) return;
    if (el.dataset.deletedForAll === "true") return;

    const body = el.querySelector('[data-role="body"]');
    if (body) body.textContent = text;

    const metaRight = el.querySelector(".meta span");
    if (metaRight && isEdited && !metaRight.querySelector(".edited")){
      const ed = document.createElement("span");
      ed.className = "edited";
      ed.textContent = "edited";
      metaRight.appendChild(ed);
    }
  }

  function applyDeletedAll(id){
    const el = msgElById.get(id);
    if (!el) return;
    el.dataset.deletedForAll = "true";
    el.querySelectorAll(".media").forEach(n => n.remove());
    const body = el.querySelector('[data-role="body"]');
    if (body){
      body.className = "deleted";
      body.textContent = "Сообщение удалено";
    }
  }

  async function loadHistory(){
    if (!token) return openAuth("login");
    if (!activeChatId) return;

    const box = $("msgs");
    box.innerHTML = "";
    msgElById.clear();
    lastMsgId = 0;

    addSystem(`📥 Loading: ${activeChatTitle}…`);

    try{
      const data = await api(`/api/messages?chat_id=${encodeURIComponent(activeChatId)}`);
      box.innerHTML = "";
      for (const m of (data.messages || [])) addMsg(m);
      scrollToBottom(box);
      updateToBottom();
      maybeMarkRead();
      refreshChats(false).catch(()=>{});
    } catch (e) {
      addSystem("❌ " + e.message);
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
      addSystem("❌ " + e.message);
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
      const res = await fetch("/api/stories", { method: "POST", headers: { "Authorization": `Bearer ${token}` }, body: fd });
      const raw = await res.text();
      const data = JSON.parse(raw || "{}");
      if (!res.ok) throw new Error(data.detail || raw);
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
      del.className = "btn";
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
    hist.innerHTML = `<b>Предыдущие аватары:</b> ${avatarHistory.length ? avatarHistory.map(a=>`<a href="${escapeHtml(a.avatar_url)}" target="_blank">${new Date((a.created_at||0)*1000).toLocaleDateString()}</a>`).join(' • ') : 'нет'}`;
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
      item.innerHTML = `<img src="${s.avatar_url || ''}" alt="${username}" /><span>${s.display_name || username}</span>`;
      item.onclick = () => window.open(s.media_url, "_blank");
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
      row.innerHTML = `<div><div class="title">${escapeHtml(c.display_name || c.username)}</div><div class="sub">@${escapeHtml(c.username)} • ${c.online ? 'в сети' : 'не в сети'}</div></div>`;
      list.appendChild(row);
    }
  }

  function openContacts(){
    if (!token) return openAuth('login');
    $("contactsOverlay").classList.add('open');
    $("contactsOverlay").setAttribute('aria-hidden','false');
    loadContacts().catch((e)=>addSystem('❌ '+(e.message||e)));
  }

  function closeContacts(){
    $("contactsOverlay").classList.remove('open');
    $("contactsOverlay").setAttribute('aria-hidden','true');
  }

  async function addContact(){
    const u = $("contactUsername").value.trim();
    if (!u) return;
    await api('/api/contacts', 'POST', { username: u });
    $("contactUsername").value = '';
    await loadContacts();
  }

  async function openChatInfo(){
    if (!activeChatId) return;
    $("chatInfoOverlay").classList.add('open');
    $("chatInfoOverlay").setAttribute('aria-hidden','false');
    $("chatInfoTitle").textContent = activeChatTitle;
    await loadChatOverview('');
  }

  function closeChatInfo(){
    $("chatInfoOverlay").classList.remove('open');
    $("chatInfoOverlay").setAttribute('aria-hidden','true');
  }

  async function loadChatOverview(keyword){
    const data = await api(`/api/chats/${encodeURIComponent(activeChatId)}/overview?q=${encodeURIComponent(keyword||'')}`);
    const msgs = data.messages || [];
    const media = data.media || [];
    const links = data.links || [];
    const members = data.members || [];
    $("chatInfoResults").innerHTML = `<b>Сообщения:</b><br>${msgs.map(m=>`${escapeHtml(m.sender)}: ${escapeHtml((m.text||'').slice(0,80))}`).join('<br>') || 'нет'}`;
    $("chatInfoMedia").innerHTML = `<b>Вложения:</b> ${media.length}`;
    $("chatInfoLinks").innerHTML = `<b>Ссылки:</b><br>${links.map(l=>escapeHtml((l.text||'').slice(0,90))).join('<br>') || 'нет'}`;
    $("chatInfoMembers").innerHTML = `<b>Участники:</b><br>${members.map(m=>`${escapeHtml(m.display_name)} (${m.online ? 'в сети' : 'не в сети'})`).join('<br>')}`;
  }

  // =========================
  // Context menu
  // =========================
  const ctx = {
    el: $("ctxMenu"),
    reply: $("ctxReply"),
    react: $("ctxReact"),
    pin: $("ctxPin"),
    edit: $("ctxEdit"),
    delMe: $("ctxDeleteMe"),
    delAll: $("ctxDeleteAll"),
    cancel: $("ctxCancel"),
    msgEl: null,
    msgId: 0,
    sender: ""
  };

  function closeCtx(){
    ctx.el.classList.remove("open");
    ctx.msgEl = null;
    ctx.msgId = 0;
    ctx.sender = "";
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

    ctx.el.style.left = `${Math.min(x, window.innerWidth - 220)}px`;
    ctx.el.style.top = `${Math.min(y, window.innerHeight - 160)}px`;
    ctx.el.classList.add("open");
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
    save.className = "btn";
    save.textContent = "Save";
    save.style.flex = "1";

    const cancel = document.createElement("button");
    cancel.className = "btn danger";
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
  ctx.react.onclick = () => { const id = ctx.msgId; closeCtx(); if (id) toggleReaction(id, "👍"); };
  ctx.pin.onclick = () => { const id = ctx.msgId; closeCtx(); if (id) togglePin(id); };
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
    toggleHidden($("btnLogout"), !token);
    toggleHidden($("btnProfile"), !token);

    const img = $("topAvatar");
    if (token && avatarUrl){
      img.src = avatarUrl;
      img.classList.add("show");
    } else {
      img.classList.remove("show");
      img.removeAttribute("src");
    }
  }

  function logout(){
    stopAllExcept(null);
    token = ""; refreshToken = ""; me = ""; avatarUrl = "";
    localStorage.removeItem("token");
    localStorage.removeItem("refresh_token");
    localStorage.removeItem("username");
    localStorage.removeItem("avatar_url");
    localStorage.removeItem("activeChatId");

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
    closeSheet();
    closeProfile();
    closeVoicePreview();
    openAuth("login");
  }

  // =========================
  // Wiring
  // =========================
  $("btnToggleSidebar").onclick = () => toggleSidebar();
  $("drawerBackdrop").onclick = () => closeSidebar();
  window.addEventListener("resize", ()=> {
    syncSidebarTopOffset();
    if (!isMobile()) closeSidebar({ restoreFocus:false });
  });
  window.addEventListener("orientationchange", syncSidebarTopOffset);

  $("btnThemeToggle").onclick = () => toggleTheme();
  $("btnOpenAuth").onclick = () => openAuth("login");
  $("btnLogout").onclick = () => logout();
  $("btnProfile").onclick = () => openProfile();
  $("btnContacts").onclick = () => openContacts();
  $("btnChatInfo").onclick = () => openChatInfo();

  $("tabLogin").onclick = () => setAuthTab("login");
  $("tabRegister").onclick = () => setAuthTab("register");
  $("btnCloseAuth").onclick = () => { if (!token) return; closeAuth(); };
  $("btnAuthSubmit").onclick = () => authSubmit();
  $("authPassword").addEventListener("keydown", (e)=>{ if (e.key === "Enter") authSubmit(); });

  $("btnOpenCreateGroup").onclick = () => openSheet("group");
  $("btnOpenCreateDM").onclick = () => openSheet("dm");
  $("btnCloseSheet").onclick = () => closeSheet();
  $("btnSheetCancel").onclick = () => closeSheet();
  $("sheetOverlay").addEventListener("click", (e)=>{ if (e.target === $("sheetOverlay")) closeSheet(); });
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
  $("btnCloseContacts").onclick = () => closeContacts();
  $("btnCloseChatInfo").onclick = () => closeChatInfo();
  $("btnAddContact").onclick = () => addContact().catch(e=> addSystem("❌ " + (e.message || e)));
  $("btnChatInfoSearch").onclick = () => loadChatOverview($("chatInfoSearch").value.trim()).catch(e=> addSystem("❌ " + (e.message || e)));
  $("contactsOverlay").addEventListener("click", (e)=>{ if (e.target === $("contactsOverlay")) closeContacts(); });
  $("chatInfoOverlay").addEventListener("click", (e)=>{ if (e.target === $("chatInfoOverlay")) closeChatInfo(); });
  $("profileOverlay").addEventListener("click", (e)=>{ if (e.target === $("profileOverlay")) closeProfile(); });
  $("btnUploadAvatar").onclick = () => uploadAvatar();
  $("btnSaveProfile").onclick = () => saveProfile();
  $("btnUploadStory").onclick = () => uploadStory();

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

  $("btnRefreshChats").onclick = () => refreshChats(false).catch(e=> addSystem("❌ " + e.message));
  $("btnLoadHistory").onclick = () => loadHistory().catch(e=> addSystem("❌ " + e.message));
  $("btnInvite").onclick = () => inviteUser();
  $("btnMute").onclick = () => muteChat();

  $("msgs").addEventListener("scroll", ()=> updateToBottom(), { passive:true });
  toBottomBtn.onclick = () => { scrollToBottom($("msgs")); updateToBottom(); };

  document.addEventListener("keydown", (e)=>{
    if (e.key === "Escape"){
      closeCtx();
      closeSidebar();
      closeSheet();
      closeProfile();
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
  setWhoami();
  requestNotificationPermissionIfNeeded();

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
      })
      .catch(() => {
        token = "";
        localStorage.removeItem("token");
        localStorage.removeItem("refresh_token");
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
  }
